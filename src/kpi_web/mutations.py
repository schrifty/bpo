"""Map HTTP bodies onto ``metrics_registry_write`` (no forked write rules)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.metrics_registry_write import (
    KPICatalogChange,
    MetricsRegistryWriteError,
    UNSET,
    add_registry_metric,
    delete_registry_metric,
    edit_registry_metric,
)

logger = logging.getLogger(__name__)


def change_to_dict(change: KPICatalogChange, *, actor: str) -> dict[str, Any]:
    """JSON shape for a catalog mutation result (CLI parity + actor audit)."""
    return {
        "action": change.action,
        "name": change.name,
        "previous_name": change.previous_name,
        "entry": change.entry,
        "path": str(change.path),
        "dry_run": change.dry_run,
        "actor": actor,
    }


def _pick(body: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in body:
            return body[key]
    return UNSET


def _as_bool(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


# Set once at creation; the web UI may not change them afterwards.
IMMUTABLE_EDIT_FIELDS: dict[str, tuple[str, ...]] = {
    "description": ("description", "clear_description", "clear-description"),
    "grain": ("grain", "clear_grain", "clear-grain"),
    "metric-generator": (
        "metric_generator",
        "metric-generator",
        "generator",
        "clear_generator",
        "clear-generator",
    ),
    "owner": ("owner", "clear_owner", "clear-owner"),
}


def reject_immutable_edits(body: dict[str, Any]) -> None:
    """Fail loud when an edit body touches a field that is fixed at creation."""
    blocked = sorted(
        field
        for field, keys in IMMUTABLE_EDIT_FIELDS.items()
        if any(key in body for key in keys)
    )
    if blocked:
        raise MetricsRegistryWriteError(
            f"{', '.join(blocked)} is immutable after a KPI is created "
            "(editable: name, tags, target)"
        )


def parse_dry_run(body: dict[str, Any] | None, query_raw: str | None) -> bool:
    if query_raw is not None and str(query_raw).strip() != "":
        return _as_bool(query_raw)
    if body and "dry_run" in body:
        return _as_bool(body.get("dry_run"))
    return False


def audit_catalog_change(change: KPICatalogChange, *, actor: str) -> None:
    """Minimal who/what audit trail (structured log; cheap and fail-open)."""
    logger.info(
        "kpi_catalog_change action=%s name=%r previous=%r actor=%s dry_run=%s path=%s",
        change.action,
        change.name,
        change.previous_name,
        actor,
        change.dry_run,
        change.path,
    )


def add_from_body(
    body: dict[str, Any],
    *,
    actor: str,
    registry_path: Path | None,
    owners_path: Path | None,
    dry_run: bool,
) -> KPICatalogChange:
    name = body.get("name")
    if name is None or not str(name).strip():
        raise MetricsRegistryWriteError("KPI name is required")
    owner = _pick(body, "owner")
    return add_registry_metric(
        str(name).strip(),
        path=registry_path,
        description=_pick(body, "description"),
        mgmt_guidance=_pick(body, "mgmt_guidance", "mgmt-guidance"),
        owner=owner,
        metric_id=_pick(body, "metric_id", "metric-id"),
        generator=_pick(body, "metric_generator", "metric-generator", "generator"),
        grain=_pick(body, "grain"),
        tags=_pick(body, "tags"),
        unit=_pick(body, "unit"),
        target=_pick(body, "target"),
        direction=_pick(body, "direction"),
        actor=actor,
        owners_path=owners_path,
        dry_run=dry_run,
    )


def edit_from_body(
    name: str,
    body: dict[str, Any],
    *,
    actor: str,
    registry_path: Path | None,
    owners_path: Path | None,
    dry_run: bool,
) -> KPICatalogChange:
    new_name_raw = body.get("new_name") or body.get("new-name")
    new_name = str(new_name_raw).strip() if new_name_raw is not None and str(new_name_raw).strip() else None
    return edit_registry_metric(
        name,
        path=registry_path,
        new_name=new_name,
        description=_pick(body, "description"),
        mgmt_guidance=_pick(body, "mgmt_guidance", "mgmt-guidance"),
        owner=_pick(body, "owner"),
        metric_id=_pick(body, "metric_id", "metric-id"),
        generator=_pick(body, "metric_generator", "metric-generator", "generator"),
        grain=_pick(body, "grain"),
        tags=_pick(body, "tags"),
        add_tags=_pick(body, "add_tags", "add-tags"),
        remove_tags=_pick(body, "remove_tags", "remove-tags"),
        unit=_pick(body, "unit"),
        target=_pick(body, "target"),
        direction=_pick(body, "direction"),
        clear_description=_as_bool(body.get("clear_description") or body.get("clear-description")),
        clear_mgmt_guidance=_as_bool(
            body.get("clear_mgmt_guidance") or body.get("clear-mgmt-guidance")
        ),
        clear_owner=_as_bool(body.get("clear_owner") or body.get("clear-owner")),
        clear_metric_id=_as_bool(body.get("clear_metric_id") or body.get("clear-metric-id")),
        clear_generator=_as_bool(body.get("clear_generator") or body.get("clear-generator")),
        clear_grain=_as_bool(body.get("clear_grain") or body.get("clear-grain")),
        clear_tags=_as_bool(body.get("clear_tags") or body.get("clear-tags")),
        clear_unit=_as_bool(body.get("clear_unit") or body.get("clear-unit")),
        clear_target=_as_bool(body.get("clear_target") or body.get("clear-target")),
        clear_direction=_as_bool(body.get("clear_direction") or body.get("clear-direction")),
        actor=actor,
        owners_path=owners_path,
        dry_run=dry_run,
    )


def delete_metric(
    name: str,
    *,
    actor: str,
    registry_path: Path | None,
    owners_path: Path | None,
    dry_run: bool,
) -> KPICatalogChange:
    return delete_registry_metric(
        name,
        path=registry_path,
        actor=actor,
        owners_path=owners_path,
        dry_run=dry_run,
    )
