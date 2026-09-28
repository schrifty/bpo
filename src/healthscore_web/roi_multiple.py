"""CS Report and Salesforce generator for Health Score ``roi_multiple``.

Numerator: previous-period inventory-action savings
(``inventoryActionPreviousReportingPeriodSavings``), summed across the
entity's matched CS Report factories. Denominator: the Customer Entity's
Salesforce ``ARR__c``. The stored value is that multiple. No weight is set
on the component, so a reading does not enter the weighted score.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from src.cs_report_client import _build_csr_site_entry, load_latest_csr_week_rows
from src.healthscore_web.store import connect, period_key_for, upsert_reading
from src.healthscore_web.usage_level import csr_site_matches_entity

logger = logging.getLogger(__name__)

METRIC_NAME = "roi_multiple"
GRAIN = "quarterly"
GENERATOR_NAME = "get_roi_multiple"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"
TAGS = ["customer-success", "healthscore", "value"]
SAVINGS_FIELD = "ia_previous_period_savings"


class RoiMultipleGeneratorError(RuntimeError):
    pass


def roi_multiple_points(multiple: float | None) -> int | None:
    """Map savings / ARR onto the draft bands. None stays unscored."""
    if multiple is None:
        return None
    if multiple >= 7:
        return 6
    if multiple >= 5:
        return 4
    if multiple >= 3:
        return 2
    return 0


def _arr(entity: dict[str, Any]) -> float | None:
    raw = entity.get("arr")
    if raw is None:
        raw = entity.get("ARR__c")
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise RoiMultipleGeneratorError(
            f"Salesforce ARR for {entity.get('id')} is not a number: {raw!r}"
        ) from exc


def _week_sites(week_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    try:
        rows = week_rows if week_rows is not None else load_latest_csr_week_rows()
    except Exception as exc:  # noqa: BLE001
        raise RoiMultipleGeneratorError(
            f"CS Report load failed for roi_multiple: {exc}"
        ) from exc
    return [
        _build_csr_site_entry(row)
        for row in rows
        if str(row.get("delta") or "").strip().lower() == "week"
    ]


def get_roi_multiple(
    *,
    entities: list[dict[str, Any]],
    week_rows: list[dict[str, Any]] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    generator: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score ROI multiple for each active Salesforce Customer Entity."""
    if not entities:
        raise RoiMultipleGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate roi_multiple"
        )
    sites = _week_sites(week_rows)
    if not sites:
        raise RoiMultipleGeneratorError(
            "CS Report week grain returned no factory rows for roi_multiple"
        )
    today = as_of or date.today()
    warnings: list[dict[str, str]] = []
    overridden: list[str] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        for entity in entities:
            entity_id = str(entity["id"])
            matched = [site for site in sites if csr_site_matches_entity(site, entity)]
            savings_values = [
                float(site[SAVINGS_FIELD])
                for site in matched
                if isinstance(site.get(SAVINGS_FIELD), (int, float))
            ]
            arr = _arr(entity)
            savings = round(sum(savings_values), 2) if savings_values else None
            error = None
            multiple = None
            if not matched:
                error = "no CS Report entity match for roi_multiple"
            elif savings is None:
                error = "CS Report matched this entity but had no previous-period savings"
            elif arr is None:
                error = "Salesforce ARR is missing"
            elif arr <= 0:
                error = "Salesforce ARR is not positive"
            else:
                multiple = round(savings / arr, 4)
            points = roi_multiple_points(multiple)
            if error:
                warnings.append(
                    {
                        "id": entity_id,
                        "name": str(entity.get("name") or ""),
                        "warning": error,
                    }
                )
                logger.warning(
                    "Health Score roi_multiple: %s for Salesforce entity %s (%s)",
                    error,
                    entity.get("name"),
                    entity_id,
                )
            meta = {
                "source_fields": [
                    "inventoryActionPreviousReportingPeriodSavings",
                    "ARR__c",
                ],
                "savings": savings,
                "arr": arr,
                "factory_count": len(matched),
                "owner": OWNER_EMAIL,
            }
            if conn is None:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "metric_name": METRIC_NAME,
                        "grain": GRAIN,
                        "period_key": period_key_for(GRAIN, today),
                        "as_of": today.isoformat(),
                        "value": multiple,
                        "points": points,
                        "generator": generator,
                        "tags": list(TAGS),
                        "meta": meta,
                        "error": error,
                    }
                )
                continue
            saved = upsert_reading(
                conn,
                entity_id=entity_id,
                entity_name=str(entity.get("name") or ""),
                metric_name=METRIC_NAME,
                grain=GRAIN,
                as_of=today,
                value=multiple,
                points=None if points is None else float(points),
                generator=generator,
                tags=TAGS,
                meta=meta,
                error=error,
            )
            if saved.get("overridden"):
                overridden.append(entity_id)
            readings.append(saved)
    finally:
        if conn is not None:
            conn.close()
    scored = sum(1 for row in readings if row.get("points") is not None)
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "metric_name": METRIC_NAME,
        "grain": GRAIN,
        "owner": OWNER_EMAIL,
        "owner_display": OWNER_NAME,
        "scored": scored,
        "unscored": len(readings) - scored,
        "overridden": len(overridden),
        "warnings": warnings,
        "readings": readings,
    }
