"""Authenticated API for the separate Customer Health Score product."""

from __future__ import annotations

import logging
import math
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
from src.healthscore_web.call_sentiment import CallSentimentGeneratorError
from src.healthscore_web.meeting_cadence import MeetingCadenceGeneratorError
from src.healthscore_web.escalation_rate import EscalationRateGeneratorError
from src.healthscore_web.sla_adherence import SlaAdherenceGeneratorError
from src.healthscore_web.ticket_volume_trend import TicketVolumeGeneratorError
from src.healthscore_web.enhancement_engagement import EnhancementEngagementGeneratorError
from src.healthscore_web.champion_login import ChampionLoginGeneratorError
from src.healthscore_web.champion_turnover import ChampionTurnoverGeneratorError
from src.healthscore_web.roi_multiple import RoiMultipleGeneratorError
from src.healthscore_web.summit_attendance import SummitAttendanceGeneratorError
from src.healthscore_web.snapshot import (
    run_call_sentiment_snapshot,
    run_meeting_cadence_snapshot,
    run_escalation_rate_snapshot,
    run_sla_adherence_snapshot,
    run_ticket_volume_snapshot,
    run_enhancement_engagement_snapshot,
    run_champion_login_snapshot,
    run_champion_turnover_snapshot,
    run_roi_multiple_snapshot,
    run_summit_attendance_snapshot,
    run_usage_level_snapshot,
    run_usage_trend_snapshot,
)
from src.healthscore_web.store import (
    clear_override,
    connect,
    latest_by_metric,
    latest_effective_by_entity,
    observations_for_entity,
    period_key_for,
    set_override,
)
from src.healthscore_web.usage_level import UsageLevelGeneratorError
from src.healthscore_web.usage_trend import UsageTrendGeneratorError
from src.kpi_web.auth import KPIWebAuthError, auth_error_response, require_user
from src.metrics_registry import normalize_owner_email
from src.salesforce_client import SalesforceClient, _CHURNED_CONTRACT_STATUS_LOWER

logger = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).resolve().parent / "static"


def _score_change_denied(user, component_key: str) -> JSONResponse | None:
    """Only the component's KPI owner may write or regenerate its score."""
    definition = component_map(load_framework()).get(component_key)
    if not definition:
        return JSONResponse(
            {"ok": False, "error": f"unknown Health Score component: {component_key}"},
            status_code=404,
        )
    owner = normalize_owner_email(definition.get("owner"))
    actor = normalize_owner_email(user.email)
    if not owner or actor != owner:
        return JSONResponse(
            {
                "ok": False,
                "error": f"only the KPI owner ({owner or 'unset'}) can change this score",
            },
            status_code=403,
        )
    return None


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


def _bounded_points(raw: float, definition: dict[str, Any]) -> float:
    """Clamp a reading to the component's point range, which may go below zero."""
    limit = float(definition["max_points"])
    floor = definition.get("min_points")
    low = float(floor) if floor is not None else 0.0
    return max(low, min(float(raw), limit))


def _score_numbers(
    framework: dict[str, Any], points_by_metric: dict[str, float | None]
) -> dict[str, Any]:
    """Provisional 0–100 score across inputs that have both weight and points."""
    configured_weight = float(framework["configured_weight"])
    contribution = 0.0
    covered_weight = 0.0
    for definition in framework["inputs"]:
        weight = definition.get("weight")
        max_points = definition.get("max_points")
        raw = points_by_metric.get(str(definition["key"]))
        if definition.get("deactivated") or raw is None or weight is None or max_points in (None, 0):
            continue
        points = _bounded_points(float(raw), definition)
        contribution += points / float(max_points) * float(weight)
        covered_weight += float(weight)
    return {
        "score": (
            round(contribution / covered_weight * 100, 2) if covered_weight else None
        ),
        "contribution": round(contribution, 4),
        "configured_weight": configured_weight,
        "covered_weight": round(covered_weight, 4),
        "coverage_pct": (
            round(covered_weight / configured_weight * 100, 2) if configured_weight else 0
        ),
    }


def _flag_is_on(value: Any) -> bool:
    """An override flag is on when its latest value is present and not zero."""
    if value is None or value == "":
        return False
    try:
        return float(value) != 0
    except (TypeError, ValueError):
        return bool(value)


def _raised_override_names(
    framework: dict[str, Any], values_by_key: dict[str, Any]
) -> list[str]:
    """Names of active override flags whose latest value is on."""
    names: list[str] = []
    for definition in framework.get("overrides") or []:
        if definition.get("deactivated"):
            continue
        if _flag_is_on(values_by_key.get(str(definition["key"]))):
            names.append(str(definition.get("name") or definition["key"]))
    return names


def _apply_override_flags(
    numbers: dict[str, Any], raised: list[str]
) -> dict[str, Any]:
    """A raised override flag sets the healthscore to 0 without changing weight."""
    numbers = dict(numbers)
    numbers["score_zeroed"] = bool(raised)
    numbers["zeroed_by"] = list(raised)
    if raised:
        numbers["score"] = 0.0
    return numbers


def shown_score(score: float | None) -> int | None:
    """Nearest integer, matching ``Math.round`` for the non-negative scores we show."""
    if score is None:
        return None
    return int(math.floor(float(score) + 0.5))


def score_band(score: float | None) -> str | None:
    """Red 0–33, yellow 34–67, green 68–100, using the rounded score."""
    shown = shown_score(score)
    if shown is None:
        return None
    if shown <= 33:
        return "red"
    if shown <= 67:
        return "yellow"
    return "green"


def _score_payload(
    framework: dict[str, Any], observations: list[dict[str, Any]]
) -> dict[str, Any]:
    latest = latest_by_metric(observations)
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
            points = _bounded_points(float(row["effective_points"]), definition)
            value = points / float(max_points) * float(weight)
            item["contribution"] = round(value, 4)
            pillar = pillars[str(definition["pillar"])]
            pillar["covered_weight"] += float(weight)
            pillar["contribution"] += value
        components.append(item)
    overrides = [
        {**definition, "latest": latest.get(str(definition["key"]))}
        for definition in framework["overrides"]
    ]
    numbers = _apply_override_flags(
        _score_numbers(
            framework,
            {
                str(definition["key"]): (latest.get(str(definition["key"])) or {}).get(
                    "effective_points"
                )
                for definition in framework["inputs"]
            },
        ),
        _raised_override_names(
            framework,
            {
                str(definition["key"]): (latest.get(str(definition["key"])) or {}).get(
                    "effective_value"
                )
                for definition in framework["overrides"]
            },
        ),
    )
    return {
        **numbers,
        "score_label": "provisional score across observed weighted inputs",
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
            points = _bounded_points(float(row["effective_points"]), definition)
            contribution += points / float(max_points) * float(weight)
            covered += float(weight)
        raised = _raised_override_names(
            framework,
            {
                str(definition["key"]): (latest.get(str(definition["key"])) or {}).get(
                    "effective_value"
                )
                for definition in framework["overrides"]
            },
        )
        if not covered and not raised:
            continue
        score = round(contribution / covered * 100, 2) if covered else None
        if raised:
            score = 0.0
        out.append(
            {
                "period_key": period,
                "score": score,
                "covered_weight": round(covered, 2),
                "score_zeroed": bool(raised),
            }
        )
    return out


async def report_page(request: Request) -> Response:
    page = STATIC_DIR / "report.html"
    if not page.is_file():
        return JSONResponse(
            {"ok": False, "error": f"Health Score report UI missing: {page}"},
            status_code=500,
        )
    return Response(page.read_text(encoding="utf-8"), media_type="text/html")


async def api_report(request: Request) -> Response:
    """Every active entity, highest healthscore times weight scored first.

    Healthscore is the 0–100 score on covered inputs. Weight scored is the
    covered weight. Their product is that entity's contribution, so a broader
    scored book ranks above a thin perfect score. Unscored entities stay last.
    """
    try:
        require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    try:
        entities = _active_salesforce_entities()
        framework = load_framework()
        conn = connect()
        try:
            points, flag_values = latest_effective_by_entity(conn)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score report failed")
        return JSONResponse(
            {"ok": False, "error": f"Health Score report failed: {exc}"},
            status_code=502,
        )
    rows = []
    for entity in entities:
        numbers = _apply_override_flags(
            _score_numbers(framework, points.get(entity["id"], {})),
            _raised_override_names(framework, flag_values.get(entity["id"], {})),
        )
        score = numbers["score"]
        weighted = None if score is None else round(float(numbers["contribution"]), 4)
        if numbers["score_zeroed"]:
            weighted = 0.0
        rows.append(
            {
                "id": entity["id"],
                "name": entity["name"],
                "score": score,
                "shown_score": shown_score(score),
                "band": score_band(score),
                "coverage_pct": numbers["coverage_pct"],
                "weighted_score": weighted,
                "score_zeroed": numbers["score_zeroed"],
            }
        )
    rows.sort(
        key=lambda row: (
            row["weighted_score"] is None,
            -(row["weighted_score"] if row["weighted_score"] is not None else 0),
            row["name"].casefold(),
        )
    )
    counts = {"red": 0, "yellow": 0, "green": 0, "unscored": 0}
    for row in rows:
        counts[row["band"] or "unscored"] += 1
    return JSONResponse(
        {
            "ok": True,
            "source": "Salesforce Account.Type = Customer Entity",
            "entity_count": len(rows),
            "counts": counts,
            "entities": rows,
        }
    )


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
        denied = _score_change_denied(user, metric_name)
        if denied:
            return denied
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
        floor = definition.get("min_points")
        low = float(floor) if floor is not None else 0.0
        if points is not None and points < low:
            raise ValueError(f"points cannot be below {low:g}")
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
    """Catalog admins may edit a component's name, pillar, weight, description, owner, grain, or deactivated flag."""
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
            description=raw["description"] if "description" in raw else _UNSET,
            owner=raw["owner"] if "owner" in raw else _UNSET,
            grain=raw["grain"] if "grain" in raw else _UNSET,
            deactivated=raw["deactivated"] if "deactivated" in raw else _UNSET,
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
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "usage_level")
    if denied:
        return denied
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


async def api_generate_usage_trend(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "usage_trend")
    if denied:
        return denied
    try:
        result = run_usage_trend_snapshot(dry_run=False)
    except UsageTrendGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score usage_trend generate failed")
        return JSONResponse(
            {"ok": False, "error": f"usage_trend generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_champion_login(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "champion_login_continuity")
    if denied:
        return denied
    try:
        result = run_champion_login_snapshot(dry_run=False)
    except ChampionLoginGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score champion_login_continuity generate failed")
        return JSONResponse(
            {"ok": False, "error": f"champion_login_continuity generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_champion_turnover(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "champion_turnover")
    if denied:
        return denied
    try:
        result = run_champion_turnover_snapshot(dry_run=False)
    except ChampionTurnoverGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score champion_turnover generate failed")
        return JSONResponse(
            {"ok": False, "error": f"champion_turnover generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_roi_multiple(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "roi_multiple")
    if denied:
        return denied
    try:
        result = run_roi_multiple_snapshot(dry_run=False)
    except RoiMultipleGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score roi_multiple generate failed")
        return JSONResponse(
            {"ok": False, "error": f"roi_multiple generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_meeting_cadence(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "meeting_cadence")
    if denied:
        return denied
    try:
        result = run_meeting_cadence_snapshot(dry_run=False)
    except MeetingCadenceGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score meeting_cadence generate failed")
        return JSONResponse(
            {"ok": False, "error": f"meeting_cadence generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_call_sentiment(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "call_sentiment")
    if denied:
        return denied
    try:
        result = run_call_sentiment_snapshot(dry_run=False)
    except CallSentimentGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score call_sentiment generate failed")
        return JSONResponse(
            {"ok": False, "error": f"call_sentiment generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_ticket_volume_trend(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "ticket_volume_trend")
    if denied:
        return denied
    try:
        result = run_ticket_volume_snapshot(dry_run=False)
    except TicketVolumeGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score ticket_volume_trend generate failed")
        return JSONResponse(
            {"ok": False, "error": f"ticket_volume_trend generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_escalation_rate(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "escalation_rate")
    if denied:
        return denied
    try:
        result = run_escalation_rate_snapshot(dry_run=False)
    except EscalationRateGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score escalation_rate generate failed")
        return JSONResponse(
            {"ok": False, "error": f"escalation_rate generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_sla_adherence(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "sla_adherence")
    if denied:
        return denied
    try:
        result = run_sla_adherence_snapshot(dry_run=False)
    except SlaAdherenceGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score sla_adherence generate failed")
        return JSONResponse(
            {"ok": False, "error": f"sla_adherence generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_enhancement_engagement(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "enhancement_engagement")
    if denied:
        return denied
    try:
        result = run_enhancement_engagement_snapshot(dry_run=False)
    except EnhancementEngagementGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score enhancement_engagement generate failed")
        return JSONResponse(
            {"ok": False, "error": f"enhancement_engagement generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


async def api_generate_summit_attendance(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    denied = _score_change_denied(user, "summit_attendance")
    if denied:
        return denied
    try:
        result = run_summit_attendance_snapshot(dry_run=False)
    except SummitAttendanceGeneratorError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Health Score summit_attendance generate failed")
        return JSONResponse(
            {"ok": False, "error": f"summit_attendance generate failed: {exc}"},
            status_code=502,
        )
    return JSONResponse(result)


def static_mount() -> StaticFiles:
    return StaticFiles(directory=str(STATIC_DIR))
