"""CSR generator for Health Score ``usage_level`` (breadth & depth).

This is not a KPI-catalog generator. It reads CS Report week rows and scores
active Salesforce Customer Entities only.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from src.cs_report_client import _build_csr_site_entry, load_latest_csr_week_rows
from src.healthscore_web.store import connect, observations_for_entity, upsert_observation
from src.salesforce_client import _customer_label_matches_text

logger = logging.getLogger(__name__)

COMPONENT_KEY = "usage_level"
GENERATOR_NAME = "get_usage_level"
OWNER_NAME = "Lindsay Brown"
OWNER_EMAIL = "lindsay.brown@leandna.com"


class UsageLevelGeneratorError(RuntimeError):
    pass


def usage_level_points(pct: float | None) -> int:
    """Map CSR usage percent onto the draft framework bands."""
    if pct is None:
        return 0
    value = float(pct)
    if value < 0:
        raise UsageLevelGeneratorError(f"usage percent cannot be negative: {value}")
    if value <= 30:
        return 1
    if value <= 50:
        return 2
    if value <= 65:
        return 3
    if value <= 80:
        return 4
    if value <= 92:
        return 5
    return 6


def _parse_automated_health(raw: Any) -> list[dict[str, Any]]:
    payload = raw
    if isinstance(raw, str) and raw.strip().startswith("["):
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def usage_percent_from_site(site: dict[str, Any]) -> tuple[float | None, str | None]:
    """Prefer a named Usage automated-health component, else Weekly Active Buyers %."""
    for item in _parse_automated_health(site.get("automated_health_scores")):
        label = " ".join(
            str(item.get(key) or "")
            for key in ("name", "metric", "kpi", "label", "type")
        ).casefold()
        if "usage" not in label:
            continue
        score = item.get("healthScore")
        if isinstance(score, (int, float)):
            return float(score), "automatedHealthScores.usage"
    buyers = site.get("weekly_active_buyers_pct")
    if isinstance(buyers, (int, float)):
        return float(buyers), "weekly_active_buyers_pct"
    return None, None


def _labels_for_entity(entity: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for key in ("name", "entity_name", "parent_name", "ultimate_parent_name"):
        raw = str(entity.get(key) or "").strip()
        if not raw:
            continue
        folded = raw.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        out.append(raw)
    return out


def csr_site_matches_entity(site: dict[str, Any], entity: dict[str, Any]) -> bool:
    csr_entity = str(site.get("entity") or "").strip()
    if not csr_entity:
        return False
    csr_folded = csr_entity.casefold()
    csr_upper = csr_entity.upper()
    for label in _labels_for_entity(entity):
        if label.casefold() == csr_folded:
            return True
        if _customer_label_matches_text(csr_upper, label):
            return True
        if _customer_label_matches_text(label.upper(), csr_entity):
            return True
    return False


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _period_from_sites(sites: list[dict[str, Any]], fallback: date) -> str:
    dates: list[str] = []
    for site in sites:
        raw = str(site.get("end_date") or "").strip()[:10]
        if len(raw) == 10:
            try:
                date.fromisoformat(raw)
            except ValueError:
                continue
            dates.append(raw)
    return max(dates) if dates else fallback.isoformat()


def _csr_week_sites(week_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    try:
        rows = week_rows if week_rows is not None else load_latest_csr_week_rows()
    except Exception as exc:  # noqa: BLE001
        raise UsageLevelGeneratorError(
            f"CS Report load failed for usage_level: {exc}"
        ) from exc
    return [
        _build_csr_site_entry(row)
        for row in rows
        if str(row.get("delta") or "").strip().lower() == "week"
    ]


def get_usage_level(
    *,
    entities: list[dict[str, Any]],
    week_rows: list[dict[str, Any]] | None = None,
    as_of: date | None = None,
    persist: bool = False,
    entered_by: str = GENERATOR_NAME,
) -> dict[str, Any]:
    """Score Usage level for each active Salesforce Customer Entity from CSR."""
    if not entities:
        raise UsageLevelGeneratorError(
            "Salesforce Customer Entity inventory is empty; cannot generate usage_level"
        )
    sites = _csr_week_sites(week_rows)
    if not sites:
        raise UsageLevelGeneratorError(
            "CS Report week grain returned no factory rows for usage_level"
        )
    today = as_of or date.today()
    unmatched: list[dict[str, str]] = []
    skipped_manual: list[str] = []
    readings: list[dict[str, Any]] = []
    conn = connect() if persist else None
    try:
        existing_by_entity: dict[str, list[dict[str, Any]]] = {}
        if conn is not None:
            for entity in entities:
                existing_by_entity[str(entity["id"])] = observations_for_entity(
                    conn, str(entity["id"])
                )
        for entity in entities:
            matched = [site for site in sites if csr_site_matches_entity(site, entity)]
            if not matched:
                unmatched.append(
                    {
                        "id": str(entity["id"]),
                        "name": str(entity.get("name") or ""),
                        "warning": "no CS Report entity match for usage_level",
                    }
                )
                logger.warning(
                    "Health Score usage_level: no CS Report match for Salesforce entity %s (%s)",
                    entity.get("name"),
                    entity.get("id"),
                )
                continue
            percents: list[float] = []
            sources: set[str] = set()
            for site in matched:
                pct, source = usage_percent_from_site(site)
                if pct is None or source is None:
                    continue
                percents.append(pct)
                sources.add(source)
            raw_value = _mean(percents)
            points = usage_level_points(raw_value)
            period_date = _period_from_sites(matched, today)
            entity_id = str(entity["id"])
            if conn is not None:
                already = [
                    row
                    for row in existing_by_entity.get(entity_id, [])
                    if row.get("component_key") == COMPONENT_KEY
                    and str(row.get("period_date")) == period_date
                    and row.get("source_mode") == "manual"
                ]
                if already:
                    skipped_manual.append(entity_id)
                    continue
                saved = upsert_observation(
                    conn,
                    entity_id=entity_id,
                    entity_name=str(entity.get("name") or ""),
                    component_key=COMPONENT_KEY,
                    period_date=period_date,
                    raw_value=raw_value,
                    points=float(points),
                    source_mode="automated",
                    note=(
                        None
                        if raw_value is not None
                        else "CSR matched this entity but had no usage percent"
                    ),
                    entered_by=entered_by,
                )
                readings.append(saved)
            else:
                readings.append(
                    {
                        "entity_id": entity_id,
                        "entity_name": entity.get("name"),
                        "component_key": COMPONENT_KEY,
                        "period_date": period_date,
                        "raw_value": raw_value,
                        "points": points,
                        "source_mode": "automated",
                        "source_fields": sorted(sources),
                        "factory_count": len(matched),
                    }
                )
    finally:
        if conn is not None:
            conn.close()
    return {
        "ok": True,
        "generator": GENERATOR_NAME,
        "component_key": COMPONENT_KEY,
        "owner": OWNER_NAME,
        "owner_email": OWNER_EMAIL,
        "scored": len(readings),
        "unmatched": len(unmatched),
        "skipped_manual": len(skipped_manual),
        "warnings": unmatched,
        "readings": readings,
    }
