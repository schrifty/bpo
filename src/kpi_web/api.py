"""HTTP handlers for the KPI web read + maintain API (workstreams C/D)."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import (
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.staticfiles import StaticFiles

from src.config import LLM_MODEL
from src.kpi_owners import (
    assert_actor_may_mutate_metric,
    KPIOwnershipError,
    load_kpi_owners_config,
    resolve_owner_cli_value,
)
from src.kpi_service import RESOLVE_MODES, ResolveMode, resolve_kpi, resolve_kpis
from src.kpi_store import clear_manual_override, connect, list_kpis, set_manual_override
from src.kpi_store_s3 import prepare_kpi_store_for_read
from src.kpi_web.auth import (
    KPIWebAuthError,
    auth_error_response,
    clear_session_cookie,
    current_user,
    encode_session,
    exchange_google_code,
    login_redirect_or_error,
    require_user,
    set_session_cookie,
    user_from_dev_login,
    user_from_google_info,
)
from src.kpi_web.situation import (
    SITUATION_SYSTEM_PROMPT,
    KpiSituationError,
    build_situation_digest,
    digest_kpi_names,
    generate_situation_analysis,
    generate_situation_chat,
    iter_situation_analysis,
)
from src.kpi_web.situation_cache import (
    load_situation_cache,
    save_situation_cache,
    situation_cache_key,
)
from src.kpi_web.mutations import (
    add_from_body,
    audit_catalog_change,
    change_to_dict,
    delete_metric,
    edit_from_body,
    parse_dry_run,
    reject_immutable_edits,
)
from src.kpi_web.serialize import (
    catalog_entry_to_dict,
    history_to_list,
    resolved_to_dict,
    sort_kpi_dicts,
)
from src.kpi_web.settings import KPIWebSettings
from src.metrics_latest import DatapointValue
from src.metrics_registry import (
    all_registry_owners,
    all_registry_tags,
    get_registry_metric,
    iter_all_metrics,
    iter_metrics_by_owner,
    iter_metrics_by_tags,
    load_metrics_registry,
    normalize_tag,
    registry_metric_grain,
    registry_metric_owner,
    registry_metric_tags,
)
from src.metrics_registry_write import MetricsRegistryWriteError

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"


def _catalog_rows(
    registry: dict[str, Any],
    *,
    owner: str | None,
    tags: list[str] | None,
) -> list[tuple[str, dict[str, Any]]]:
    """Filter catalog rows by owner and/or AND tag-set (public registry APIs)."""
    if owner:
        rows = iter_metrics_by_owner(owner, registry=registry)
        if tags:
            need = [normalize_tag(t) for t in tags if normalize_tag(t)]
            if need:
                rows = [
                    (name, entry)
                    for name, entry in rows
                    if all(tag in registry_metric_tags(entry) for tag in need)
                ]
        return rows
    if tags:
        return iter_metrics_by_tags(tags, registry=registry)
    return iter_all_metrics(registry=registry)


def _settings(request: Request) -> KPIWebSettings:
    return request.app.state.settings


def _registry(request: Request) -> dict[str, Any]:
    settings = _settings(request)
    return load_metrics_registry(path=settings.registry_path)


def _parse_mode(raw: str | None) -> ResolveMode:
    mode = (raw or "stored").strip().lower() or "stored"
    if mode not in RESOLVE_MODES:
        raise ValueError(f"mode must be one of {RESOLVE_MODES}, got {mode!r}")
    return mode  # type: ignore[return-value]


def _parse_tags(raw: str | None) -> list[str] | None:
    if raw is None or not str(raw).strip():
        return None
    tags: list[str] = []
    for part in str(raw).replace(";", ",").split(","):
        tag = normalize_tag(part)
        if tag and tag not in tags:
            tags.append(tag)
    return tags or None


def _truthy_query(raw: str | None) -> bool:
    if raw is None:
        return False
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _open_store(settings: KPIWebSettings) -> sqlite3.Connection:
    path = prepare_kpi_store_for_read(
        None,
        skip_s3=settings.skip_s3,
    )
    return connect(path)


def _history_from_store(
    conn: sqlite3.Connection,
    metric_name: str,
    entry: dict[str, Any],
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    gen = str(entry.get("metric-generator") or "").strip()
    grain = registry_metric_grain(entry) if gen else None
    rows = list_kpis(conn, metric_name=metric_name, grain=grain, limit=max(1, limit))
    points: list[DatapointValue] = []
    for i, row in enumerate(rows):
        obs = row.effective_observation
        if obs.ok or i == 0:
            points.append(
                DatapointValue(
                    date=(obs.as_of or row.period_key or "").strip(),
                    value=obs.display_value,
                )
            )
    return history_to_list(tuple(points[:limit]))


async def api_health(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True, "service": "cortex-kpi-web"})


async def api_me(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    owners = load_kpi_owners_config(path=settings.owners_path)
    lead = owners.lead_for(user.email)
    return JSONResponse(
        {
            "ok": True,
            "email": user.email,
            "name": user.name,
            "picture": user.picture,
            "is_catalog_admin": user.is_catalog_admin,
            "auth_method": user.auth_method,
            "permissions": {
                "read": "all",
                "edit": "all" if user.is_catalog_admin else "own",
            },
            "packs": list(lead.packs) if lead else [],
            "catalog_admin": owners.catalog_admin,
        }
    )


async def api_meta(request: Request) -> Response:
    try:
        require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    try:
        reg = _registry(request)
        owners_cfg = load_kpi_owners_config(path=settings.owners_path)
    except Exception as exc:  # noqa: BLE001 — fail loud to the client
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    leads = [
        {
            "email": lead.email,
            "display_name": lead.display_name,
            "packs": list(lead.packs),
        }
        for lead in owners_cfg.leads
    ]
    packs = [
        {
            "id": pack.id,
            "description": pack.description,
            "required_any_tags": list(pack.required_any_tags),
        }
        for pack in owners_cfg.topic_packs.values()
    ]
    return JSONResponse(
        {
            "ok": True,
            "owners": [{"email": e, "count": n} for e, n in all_registry_owners(registry=reg)],
            "tags": [{"tag": t, "count": n} for t, n in all_registry_tags(registry=reg)],
            "catalog_admin": owners_cfg.catalog_admin,
            "leads": leads,
            "topic_packs": packs,
            "modes": list(RESOLVE_MODES),
            "default_mode": "stored",
        }
    )


async def api_list_kpis(request: Request) -> Response:
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    q = request.query_params
    try:
        mode = _parse_mode(q.get("mode"))
        tags = _parse_tags(q.get("tag") or q.get("tags"))
        resolve_values = _truthy_query(q.get("values") or q.get("resolve"))
        owner_raw = q.get("owner")
        owner = resolve_owner_cli_value(
            owner_raw,
            actor=user.email,
            owners=load_kpi_owners_config(path=settings.owners_path),
            default_to_me=False,
        )
        reg = _registry(request)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    # Catalog-only list (fast) unless values=1.
    if not resolve_values:
        rows = _catalog_rows(reg, owner=owner, tags=tags)
        items = sort_kpi_dicts(
            [catalog_entry_to_dict(name, entry) for name, entry in rows]
        )
        return JSONResponse(
            {
                "ok": True,
                "mode": mode,
                "resolved": False,
                "count": len(items),
                "filters": {"owner": owner, "tags": tags or []},
                "kpis": items,
            }
        )

    store_conn: sqlite3.Connection | None = None
    try:
        if mode == "stored":
            store_conn = _open_store(settings)
        items = [
            resolved_to_dict(row, include_history=False)
            for row in resolve_kpis(
                tags=tags,
                owner=owner,
                mode=mode,
                registry=reg,
                store_conn=store_conn,
                skip_s3=settings.skip_s3,
            )
        ]
    except Exception as exc:  # noqa: BLE001 — surface store/generator failures
        logger.exception("KPI list resolve failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    finally:
        if store_conn is not None:
            store_conn.close()

    return JSONResponse(
        {
            "ok": True,
            "mode": mode,
            "resolved": True,
            "count": len(items),
            "filters": {"owner": owner, "tags": tags or []},
            "kpis": sort_kpi_dicts(items),
        }
    )


def _situation_digest(request: Request, user: Any) -> dict[str, Any]:
    settings = _settings(request)
    owners = load_kpi_owners_config(path=settings.owners_path)
    lead = owners.lead_for(user.email)
    viewer = {
        "email": user.email,
        "name": user.name,
        "is_catalog_admin": user.is_catalog_admin,
        "packs": list(lead.packs) if lead else [],
    }
    store_conn: sqlite3.Connection | None = None
    try:
        store_conn = _open_store(settings)
        return build_situation_digest(store_conn, _registry(request), viewer=viewer)
    finally:
        if store_conn is not None:
            store_conn.close()


def _situation_cache_key(digest: dict[str, Any]) -> str:
    model = (
        str(LLM_MODEL) if str(LLM_MODEL).startswith("claude") else "claude-sonnet-4-6"
    )
    return situation_cache_key(
        digest,
        prompt=SITUATION_SYSTEM_PROMPT,
        model=model,
    )


async def api_kpi_situation(request: Request) -> Response:
    """GET /api/situation — Claude briefing vs week-ago and month-ago stored readings."""
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    try:
        digest = _situation_digest(request, user)
        key = _situation_cache_key(digest)
        cached = load_situation_cache(key)
        if cached:
            analysis = cached["analysis"]
        else:
            analysis = generate_situation_analysis(digest)
            try:
                cached = save_situation_cache(key, analysis)
            except OSError:
                logger.exception("Could not persist KPI situation cache")
    except KpiSituationError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI situation briefing failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    return JSONResponse(
        {
            "ok": True,
            "analysis": analysis,
            "as_of": digest.get("as_of"),
            "kpi_count": digest.get("kpi_count"),
            "kpi_names": digest_kpi_names(digest),
            "cached": bool(cached),
            "cached_at": cached.get("created_at") if cached else None,
        }
    )


async def api_kpi_situation_stream(request: Request) -> Response:
    """GET /api/situation/stream — same briefing as NDJSON, emitted as Claude writes it.

    The briefing takes ~30s to finish, so the UI reads this instead of waiting on
    the JSON endpoint. Setup errors still fail loud as a 500; once the body has
    started the only way to report a failure is an in-band ``error`` event.
    """
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    try:
        digest = _situation_digest(request, user)
        key = _situation_cache_key(digest)
        cached = load_situation_cache(key)
    except KpiSituationError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI situation digest failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)

    def events():
        def event(payload: dict[str, Any]) -> str:
            return json.dumps(payload, default=str) + "\n"

        yield event(
            {
                "type": "start",
                "as_of": digest.get("as_of"),
                "kpi_count": digest.get("kpi_count"),
                "kpi_names": digest_kpi_names(digest),
                "cached": bool(cached),
                "cached_at": cached.get("created_at") if cached else None,
            }
        )
        if cached:
            yield event({"type": "delta", "text": cached["analysis"]})
            yield event({"type": "done"})
            return
        chunks: list[str] = []
        try:
            for chunk in iter_situation_analysis(digest):
                chunks.append(chunk)
                yield event({"type": "delta", "text": chunk})
        except KpiSituationError as exc:
            yield event({"type": "error", "error": str(exc)})
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("KPI situation stream failed")
            yield event({"type": "error", "error": str(exc)})
            return
        analysis = "".join(chunks).strip()
        try:
            save_situation_cache(key, analysis)
        except (OSError, ValueError):
            logger.exception("Could not persist KPI situation cache")
        yield event({"type": "done"})

    return StreamingResponse(
        events(),
        media_type="application/x-ndjson",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


async def api_kpi_situation_chat(request: Request) -> Response:
    """POST /api/situation/chat — grounded follow-up about the current KPI digest."""
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
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
    message = str(raw.get("message") or "").strip()
    history = raw.get("history") or []
    if not message:
        return JSONResponse(
            {"ok": False, "error": "message is required"},
            status_code=400,
        )
    if len(message) > 2000:
        return JSONResponse(
            {"ok": False, "error": "message must be 2000 characters or fewer"},
            status_code=400,
        )
    if not isinstance(history, list):
        return JSONResponse(
            {"ok": False, "error": "history must be a list"},
            status_code=400,
        )
    try:
        digest = _situation_digest(request, user)
        answer = generate_situation_chat(digest, message, history=history)
    except KpiSituationError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI situation chat failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    return JSONResponse(
        {
            "ok": True,
            "answer": answer,
            "kpi_names": digest_kpi_names(digest),
        }
    )


async def api_kpi_detail(request: Request) -> Response:
    try:
        require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    name = unquote(request.path_params.get("name") or "").strip()
    if not name:
        return JSONResponse({"ok": False, "error": "metric name required"}, status_code=400)
    q = request.query_params
    try:
        mode = _parse_mode(q.get("mode"))
        history_limit = int((q.get("history") or "12").strip() or "12")
        reg = _registry(request)
        found = get_registry_metric(name, registry=reg)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    if found is None:
        return JSONResponse(
            {"ok": False, "error": f"KPI not found: {name!r}"},
            status_code=404,
        )
    metric_name, entry = found

    store_conn: sqlite3.Connection | None = None
    history_warning: str | None = None
    try:
        if mode == "stored":
            store_conn = _open_store(settings)
        else:
            # Best-effort history for live/leandna detail; never invent values.
            try:
                store_conn = _open_store(settings)
            except Exception as exc:  # noqa: BLE001
                history_warning = f"KPI store unavailable for history: {exc}"
                store_conn = None

        row = resolve_kpi(
            metric_name,
            entry,
            mode=mode,
            registry=reg,
            store_conn=store_conn if mode == "stored" else None,
            recent_count=max(1, history_limit),
        )
        payload = resolved_to_dict(row, include_history=True)
        if mode != "stored" and store_conn is not None:
            payload["history"] = _history_from_store(
                store_conn, metric_name, entry, limit=history_limit
            )
        elif mode != "stored" and not payload.get("history"):
            payload["history"] = []
        if history_warning:
            payload["history_warning"] = history_warning
        # Fail loud: never substitute a placeholder value when observation errored.
        if row.observation.error:
            payload["value_status"] = "error"
        elif not row.observation.ok:
            payload["value_status"] = "empty"
        else:
            payload["value_status"] = "ok"
        return JSONResponse({"ok": True, "mode": mode, "kpi": payload})
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI detail resolve failed for %s", metric_name)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    finally:
        if store_conn is not None:
            store_conn.close()


def _write_error_response(exc: MetricsRegistryWriteError) -> JSONResponse:
    """Map catalog write/authz failures to 400/403/404 (fail loud, no silent skip)."""
    msg = str(exc)
    lower = msg.lower()
    status = 400
    if "unauthorized" in lower or "may not" in lower or "only create" in lower:
        status = 403
    elif "not found" in lower:
        status = 404
    return JSONResponse({"ok": False, "error": msg}, status_code=status)


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        raw = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise MetricsRegistryWriteError(f"request body must be JSON: {exc}") from exc
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise MetricsRegistryWriteError("request body must be a JSON object")
    return raw


async def api_kpi_add(request: Request) -> Response:
    """POST /api/kpis — add a catalog row (same YAML shape as ``cortex kpi add``)."""
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    try:
        body = await _json_body(request)
        dry_run = parse_dry_run(body, request.query_params.get("dry_run"))
        change = add_from_body(
            body,
            actor=user.email,
            registry_path=settings.registry_path,
            owners_path=settings.owners_path,
            dry_run=dry_run,
        )
    except MetricsRegistryWriteError as exc:
        return _write_error_response(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI add failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    audit_catalog_change(change, actor=user.email)
    return JSONResponse(
        {"ok": True, "change": change_to_dict(change, actor=user.email)},
        status_code=200 if change.dry_run else 201,
    )


async def api_kpi_edit(request: Request) -> Response:
    """PATCH /api/kpis/{name} — edit a catalog row (``cortex kpi edit`` parity)."""
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    name = unquote(request.path_params.get("name") or "").strip()
    if not name:
        return JSONResponse({"ok": False, "error": "metric name required"}, status_code=400)
    try:
        body = await _json_body(request)
        reject_immutable_edits(body)
        dry_run = parse_dry_run(body, request.query_params.get("dry_run"))
        change = edit_from_body(
            name,
            body,
            actor=user.email,
            registry_path=settings.registry_path,
            owners_path=settings.owners_path,
            dry_run=dry_run,
        )
    except MetricsRegistryWriteError as exc:
        return _write_error_response(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI edit failed for %s", name)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    audit_catalog_change(change, actor=user.email)
    return JSONResponse({"ok": True, "change": change_to_dict(change, actor=user.email)})


def _parse_numeric_value(raw: Any) -> float:
    if isinstance(raw, bool) or raw is None:
        raise MetricsRegistryWriteError("value must be a number")
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    if not text:
        raise MetricsRegistryWriteError("value must be a number")
    try:
        return float(text)
    except ValueError as exc:
        raise MetricsRegistryWriteError(f"value must be a number, got {raw!r}") from exc


def _override_payload(stored: Any) -> dict[str, Any]:
    obs = stored.effective_observation
    return {
        "value": obs.display_value,
        "generated_value": stored.observation.display_value,
        "overridden": stored.is_overridden,
        "override_by": stored.override.by if stored.override else None,
        "override_at": stored.override.at if stored.override else None,
    }


async def api_kpi_set_value(request: Request) -> Response:
    """PUT /api/kpis/{name}/value — set or clear a manual override (fail loud).

    The generated reading is never overwritten: ``{"value": n}`` shadows it and
    ``{"value": null}`` restores it.
    """
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    name = unquote(request.path_params.get("name") or "").strip()
    if not name:
        return JSONResponse({"ok": False, "error": "metric name required"}, status_code=400)
    try:
        body = await _json_body(request)
        if "value" not in body:
            raise MetricsRegistryWriteError("value is required")
        raw_value = body.get("value")
        clearing = raw_value is None or (isinstance(raw_value, str) and not raw_value.strip())
        value = None if clearing else _parse_numeric_value(raw_value)
        period_key = body.get("period_key")
        period = str(period_key).strip() if period_key not in (None, "") else None
        reg = _registry(request)
        found = get_registry_metric(name, registry=reg)
        if found is None:
            return JSONResponse(
                {"ok": False, "error": f"KPI not found: {name!r}"},
                status_code=404,
            )
        metric_name, entry = found
        owners = load_kpi_owners_config(path=settings.owners_path)
        assert_actor_may_mutate_metric(
            user.email,
            existing_owner=registry_metric_owner(entry),
            new_owner=registry_metric_owner(entry),
            action="edit",
            owners=owners,
        )
        store_conn = _open_store(settings)
        try:
            if clearing:
                stored = clear_manual_override(
                    store_conn,
                    metric_name=metric_name,
                    grain=registry_metric_grain(entry),
                    period_key=period,
                )
            else:
                stored = set_manual_override(
                    store_conn,
                    metric_name=metric_name,
                    grain=registry_metric_grain(entry),
                    value=value,
                    period_key=period,
                    tags=tuple(registry_metric_tags(entry)),
                    generator=str(entry.get("metric-generator") or "").strip() or None,
                    actor=user.email,
                )
        finally:
            store_conn.close()
        if stored is None:
            raise MetricsRegistryWriteError(
                f"no stored reading for {metric_name!r} — nothing to override"
            )
    except KPIOwnershipError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=403)
    except MetricsRegistryWriteError as exc:
        return _write_error_response(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI value write failed for %s", name)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    logger.info(
        "kpi_override_write name=%r grain=%s period=%s actor=%s value=%s cleared=%s",
        metric_name,
        stored.grain,
        stored.period_key,
        user.email,
        value,
        clearing,
    )
    return JSONResponse(
        {
            "ok": True,
            "name": metric_name,
            "grain": stored.grain,
            "period_key": stored.period_key,
            "actor": user.email,
            **_override_payload(stored),
        }
    )


async def api_kpi_delete(request: Request) -> Response:
    """DELETE /api/kpis/{name} — remove a catalog row (``cortex kpi delete`` parity)."""
    try:
        user = require_user(request)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    settings = _settings(request)
    name = unquote(request.path_params.get("name") or "").strip()
    if not name:
        return JSONResponse({"ok": False, "error": "metric name required"}, status_code=400)
    try:
        # Optional empty JSON body; dry-run via query is preferred for DELETE.
        body: dict[str, Any] = {}
        if (request.headers.get("content-type") or "").lower().startswith("application/json"):
            body = await _json_body(request)
        dry_run = parse_dry_run(body, request.query_params.get("dry_run"))
        change = delete_metric(
            name,
            actor=user.email,
            registry_path=settings.registry_path,
            owners_path=settings.owners_path,
            dry_run=dry_run,
        )
    except MetricsRegistryWriteError as exc:
        return _write_error_response(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("KPI delete failed for %s", name)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
    audit_catalog_change(change, actor=user.email)
    return JSONResponse({"ok": True, "change": change_to_dict(change, actor=user.email)})


async def auth_login(request: Request) -> Response:
    return login_redirect_or_error(request)


async def auth_dev_login(request: Request) -> Response:
    settings = _settings(request)
    email = request.query_params.get("email")
    try:
        user = user_from_dev_login(settings=settings, email=email)
        token = encode_session(user, settings=settings)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    next_url = request.query_params.get("next") or "/"
    if not next_url.startswith("/"):
        next_url = "/"
    response = RedirectResponse(next_url, status_code=302)
    set_session_cookie(response, token, settings=settings)
    return response


async def auth_callback(request: Request) -> Response:
    settings = _settings(request)
    q = request.query_params
    err = q.get("error")
    if err:
        desc = q.get("error_description") or err
        return JSONResponse(
            {"ok": False, "error": f"Google OAuth error: {desc}"},
            status_code=400,
        )
    code = q.get("code")
    state = q.get("state")
    expected = request.cookies.get("cortex_kpi_oauth_state")
    if not code:
        return JSONResponse({"ok": False, "error": "missing OAuth code"}, status_code=400)
    if not state or not expected or state != expected:
        return JSONResponse(
            {"ok": False, "error": "invalid OAuth state (try /auth/login again)"},
            status_code=400,
        )
    try:
        info = exchange_google_code(code, settings=settings)
        user = user_from_google_info(info, settings=settings)
        token = encode_session(user, settings=settings)
    except KPIWebAuthError as exc:
        return auth_error_response(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("OAuth callback failed")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)
    response = RedirectResponse("/", status_code=302)
    set_session_cookie(response, token, settings=settings)
    response.delete_cookie("cortex_kpi_oauth_state", path="/")
    return response


async def auth_logout(request: Request) -> Response:
    settings = _settings(request)
    response = RedirectResponse("/", status_code=302)
    clear_session_cookie(response, settings=settings)
    return response


async def auth_status(request: Request) -> Response:
    """Unauthenticated probe used by the UI to show login options."""
    settings = _settings(request)
    try:
        user = current_user(request)
    except KPIWebAuthError as exc:
        return JSONResponse(
            {
                "ok": True,
                "authenticated": False,
                "error": exc.message,
                "google_configured": settings.google_oauth_configured,
                "dev_auth_enabled": settings.allow_dev_auth,
            }
        )
    return JSONResponse(
        {
            "ok": True,
            "authenticated": user is not None,
            "email": user.email if user else None,
            "google_configured": settings.google_oauth_configured,
            "dev_auth_enabled": settings.allow_dev_auth,
        }
    )


async def index_page(request: Request) -> Response:
    index = STATIC_DIR / "index.html"
    if not index.is_file():
        return JSONResponse(
            {"ok": False, "error": f"UI missing: {index}"},
            status_code=500,
        )
    return Response(index.read_text(encoding="utf-8"), media_type="text/html")


def static_mount() -> StaticFiles:
    return StaticFiles(directory=str(STATIC_DIR))
