"""Authenticated API for the separate Customer Health Score product."""

from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.staticfiles import StaticFiles

from src.healthscore_web.framework import (
    HealthScoreFrameworkError,
    _UNSET,
    component_map,
    load_framework,
    update_component,
)
from src.healthscore_web.snapshot import run_usage_level_snapshot
from src.healthscore_web.store import (
    clear_override,
    connect,
    latest_by_metric,
    observations_for_entity,
    period_key_for,
    set_override,
)
from src.healthscore_web.usage_level import UsageLevelGeneratorError
from src.kpi_web.auth import KPIWebAuthError, auth_error_response, require_user
from src.salesforce_client import SalesforceClient, _CHURNED_CONTRACT_STATUS_LOWER

logger = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).resolve().parent / "static"


def _active_salesforce_entities() -> list[dict[str, Any]]:
    """Active Customer Entity records; Salesforce is the only inventory authority."""
    rows = SalesforceClient().get_entity_accounts()
    entities: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("Contract_Status__c") or "").strip()
        if status.lower() in _CHURNED_CONTRACT_STATUS_LOWER:
            continue
        entity_id = str(row.get("Id") or "").strip()
        name = str(row.get("Name") or "").strip()
        if not entity_id or not name:
            continue
        entities.append(
            {
                "id": entity_id,
                "name": name,
                "entity_name": row.get("LeanDNA_Entity_Name__c"),
                "parent_name": row.get("parent_name"),
                "ultimate_parent_name": row.get("ultimate_parent_name"),
                "contract_status": status or None,
                "contract_end_date": row.get("Contract_Contract_End_Date__c"),
                "arr": row.get("ARR__c"),
            }
        )
    entities.sort(key=lambda row: row["name"].casefold())
    return entities


def _score_payload(
    framework: dict[str, Any], observations: list[dict[str, Any]]
) -> dict[str, Any]:
    latest = latest_by_metric(observations)
    configured_weight = float(framework["configured_weight"])
    contribution = 0.0
    covered_weight = 0.0
    components: list[dict[str, Any]] = []
    pillars: dict[str, dict[str, float]] = defaultdict(
        lambda: {"configured_weight": 0.0, "covered_weight": 0.0, "contribution": 0.0}
    )
    for definition in framework["inputs"]:
        row = latest.get(str(definition["key"]))
        weight = definition.get("weight")
        max_points = definition.get("max_points")
        item = {**definition, "latest": row, "contribution": None}
        if weight is not None:
            pillars[str(definition["pillar"])]["configured_weight"] += float(weight)
        if (
            row
            and row.get("effective_points") is not None
            and weight is not None
            and max_points not in (None, 0)
        ):
            points = max(0.0, min(float(row["effective_points"]), float(max_points)))
            value = points / float(max_points) * float(weight)
            item["contribution"] = round(value, 4)
            contribution += value
            covered_weight += float(weight)
            pillar = pillars[str(definition["pillar"])]
            pillar["covered_weight"] += float(weight)
            pillar["contribution"] += value
        components.append(item)
    overrides = [
        {**definition, "latest": latest.get(str(definition["key"]))}
        for definition in framework["overrides"]
    ]
    return {
        "score": (
            round(contribution / covered_weight * 100, 2)
            if covered_weight
            else None
        ),
        "score_label": "provisional score across observed weighted inputs",
        "contribution": round(contribution, 4),
        "configured_weight": configured_weight,
        "covered_weight": round(covered_weight, 4),
        "coverage_pct": round(covered_weight / configured_weight * 100, 2),
        "components": components,
        "overrides": overrides,
        "pillars": [
            {"name": name, **{k: round(v, 4) for k, v in values.items()}}
            for name, values in pillars.items()
        ],
        "history": _score_history(framework, observations),
    }


def _score_history(
    framework: dict[str, Any], observations: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    periods = sorted({str(row["period_key"]) for row in observations})
    out: list[dict[str, Any]] = []
    for period in periods:
        prior = [row for row in observations if str(row["period_key"]) <= period]
        latest = latest_by_metric(prior)
        contribution = 0.0
        covered = 0.0
        for definition in framework["inputs"]:
            row = latest.get(str(definition["key"]))
            weight = definition.get("weight")
            max_points = definition.get("max_points")
            if (
                not row
                or row.get("effective_points") is None
                or weight is None
                or not max_points
            ):
                continue
            points = max(0.0, min(float(row["effective_points"]), float(max_points)))
            contribution += points / float(max_points) * float(weight)
            covered += float(weight)
        if covered:
            out.append(
                {
                    "period_key": period,
                    "score": round(contribution / covered * 100, 2),
                    "covered_weight": round(covered, 2),
                }
            )
    return out


async def index_page(request: Request) -> Response:
    index = STATIC_DIR / "index.html"
    if not index.is_file():
        return JSONResponse(
            {"ok": False, "error": f"Health Score UI missing: {index}"},
            status_code=500,
        )
    return Response(index.read_text(encoding="utf-8"), media_type="text/html")


async def api_framework(request: Request) -> Response:
    try:
        require_user(request)
        return JSONResponse({"ok": True, "framework": load_framework()})
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score framework load failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


async def api_entities(request: Request) -> Response:
    try:
        require_user(request)
        entities = _active_salesforce_entities()
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score Salesforce entity load failed")
        return JSONResponse(
            {
                "ok": False,
                "error": f"Salesforce Customer Entity inventory failed: {exc}",
            },
            status_code=502,
        )
    return JSONResponse(
        {
            "ok": True,
            "source": "Salesforce Account.Type = Customer Entity",
            "entities": entities,
        }
    )


async def api_entity_score(request: Request) -> Response:
    try:
        require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    entity_id = str(request.path_params.get("entity_id") or "").strip()
    try:
        entities = _active_salesforce_entities()
        if not any(row["id"] == entity_id for row in entities):
            return JSONResponse(
                {
                    "ok": False,
                    "error": "entity is not an active Salesforce Customer Entity",
                },
                status_code=404,
            )
        framework = load_framework()
        conn = connect()
        try:
            observations = observations_for_entity(conn, entity_id)
        finally:
            conn.close()
        return JSONResponse(
            {
                "ok": True,
                "entity_id": entity_id,
                "observations": observations,
                **_score_payload(framework, observations),
            }
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score entity score failed")
        return JSONResponse(
            {
                "ok": False,
                "error": f"Health Score or Salesforce entity load failed: {exc}",
            },
            status_code=502,
        )


async def api_set_component(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    entity_id = str(request.path_params.get("entity_id") or "").strip()
    metric_name = str(request.path_params.get("component_key") or "").strip()
    try:
        raw = await request.json()
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(
            {"ok": False, "error": f"request body must be JSON: {exc}"},
            status_code=400,
        )
    if not isinstance(raw, dict):
        return JSONResponse(
            {"ok": False, "error": "request body must be a JSON object"},
            status_code=400,
        )
    try:
        framework = load_framework()
        definition = component_map(framework).get(metric_name)
        if not definition:
            raise ValueError(f"unknown Health Score component: {metric_name}")
        grain = definition.get("grain")
        if not grain:
            raise ValueError(
                f"{metric_name} has no grain yet; the framework defines an event "
                f"cadence ({definition.get('cadence_note') or 'unspecified'}) instead"
            )
        entities = _active_salesforce_entities()
        entity = next((row for row in entities if row["id"] == entity_id), None)
        if not entity:
            raise ValueError(
                "entity is not an active Salesforce Customer Entity"
            )
        points_raw = raw.get("points")
        points = None if points_raw in (None, "") else float(points_raw)
        max_points = definition.get("max_points")
        if points is not None and points < 0:
            raise ValueError("points cannot be negative")
        if points is not None and max_points is not None and points > float(max_points):
            raise ValueError(f"points cannot exceed {max_points}")
        value_raw = raw.get("value", raw.get("raw_value"))
        value = None if value_raw in (None, "") else float(value_raw)
        as_of = str(raw.get("as_of") or "").strip() or None
        period_key = str(raw.get("period_key") or "").strip() or None
        conn = connect()
        try:
            if value is None and points is None:
                # Same contract as the KPI page: a null override drops the
                # manual reading so the generated one shows again.
                key = period_key or period_key_for(grain, _as_date(as_of))
                clear_override(
                    conn,
                    entity_id=entity_id,
                    metric_name=metric_name,
                    grain=grain,
                    period_key=key,
                )
                latest = latest_by_metric(observations_for_entity(conn, entity_id))
                row = latest.get(metric_name)
                saved = row if row and row.get("period_key") == key else None
                return JSONResponse({"ok": True, "cleared": True, "observation": saved})
            saved = set_override(
                conn,
                entity_id=entity_id,
                entity_name=entity["name"],
                metric_name=metric_name,
                grain=grain,
                as_of=as_of,
                period_key=period_key,
                value=value,
                points=points,
                by=user.email,
                note=str(raw.get("note") or "").strip() or None,
            )
        finally:
            conn.close()
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score component write failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    return JSONResponse({"ok": True, "observation": saved})


def _as_date(raw: str | None):
    from datetime import date

    if not raw:
        return date.today()
    try:
        return date.fromisoformat(raw[:10])
    except ValueError as exc:
        raise ValueError("as_of must be YYYY-MM-DD") from exc


async def api_update_framework_component(request: Request) -> Response:
    """Catalog admins may rename an input and change its pillar or weight."""
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    if not user.is_catalog_admin:
        return JSONResponse(
            {"ok": False, "error": "only the catalog admin can edit the framework"},
            status_code=403,
        )
    key = str(request.path_params.get("component_key") or "").strip()
    try:
        raw = await request.json()
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(
            {"ok": False, "error": f"request body must be JSON: {exc}"},
            status_code=400,
        )
    if not isinstance(raw, dict):
        return JSONResponse(
            {"ok": False, "error": "request body must be a JSON object"},
            status_code=400,
        )
    try:
        framework = update_component(
            key,
            name=raw["name"] if "name" in raw else _UNSET,
            pillar=raw["pillar"] if "pillar" in raw else _UNSET,
            weight=raw["weight"] if "weight" in raw else _UNSET,
        )
    except HealthScoreFrameworkError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score framework edit failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    logger.info("Health Score framework %s edited by %s: %s", key, user.email, sorted(raw))
    return JSONResponse(
        {"ok": True, "framework": framework, "component": component_map(framework).get(key)}
    )


async def api_generate_usage_level(request: Request) -> Response:
    try:
        require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    try:
        result = run_usage_level_snapshot(dry_run=False)
    except UsageLevelGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score usage_level generate failed")
        return JSONResponse(
            {"ok": False, "error": f"usage_level generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


def static_mount() -> StaticFiles:
    return StaticFiles(directory=str(STATIC_DIR))
