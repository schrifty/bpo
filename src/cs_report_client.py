"""Client for the Customer Success Report exported to Google Drive.

Reads the latest CS Report spreadsheet from the 'Data Exports' shared drive,
parses the JSON-encoded KPI values, and provides per-customer summaries for
platform health, supply chain metrics, and ROI/value metrics.

Cross-validates overlapping fields with Pendo data via the QA registry.
"""

from __future__ import annotations

import io
import json
import threading
from typing import Any

from .config import logger
from .qa import qa

# Shared Drive ID and folder for the CS Report
_DATA_EXPORTS_DRIVE_ID = "0AHL7kClvi-JmUk9PVA"
_CS_REPORT_FOLDER_ID = "16922c1MWTKNx3Pw1W7bbUARC_q2aat0Z"

_cache: dict[str, Any] | None = None
_cache_lock = threading.Lock()


def _get_drive():
    from .slides_api import _get_service
    _x, drive, _sh = _get_service()
    return drive


def _parse_kpi(raw) -> dict[str, Any] | None:
    """Parse a JSON-encoded KPI cell into a dict with startValue/endValue/delta."""
    if raw is None:
        return None
    try:
        d = json.loads(str(raw))
        if isinstance(d, dict) and not d.get("empty", False):
            return d
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _kpi_end(raw) -> float | None:
    """Extract the end-of-period value from a KPI cell."""
    d = _parse_kpi(raw)
    if d is None:
        return None
    v = d.get("endValue")
    if v is None:
        v = d.get("startValue")
    return float(v) if v is not None else None


def _kpi_delta_pct(raw) -> float | None:
    """Extract the delta percentage from a KPI cell."""
    d = _parse_kpi(raw)
    if d is None:
        return None
    v = d.get("deltaPercent")
    return float(v) if v is not None else None


def check_reachable() -> None:
    """Verify the CS Report folder on Drive is reachable. Raises if Drive or folder is down."""
    drive = _get_drive()
    q = f"'{_CS_REPORT_FOLDER_ID}' in parents and trashed = false"
    drive.files().list(
        q=q,
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="drive",
        driveId=_DATA_EXPORTS_DRIVE_ID,
        pageSize=1,
    ).execute()


def _fetch_latest_report() -> list[dict[str, Any]]:
    """Download the latest CS Report from Drive and parse all rows.

    Thread-safe: a lock ensures only one thread fetches/parses the XLSX while
    others wait for the cached result.
    """
    global _cache
    if _cache is not None:
        return _cache["rows"]

    with _cache_lock:
        # Double-check after acquiring lock (another thread may have populated it)
        if _cache is not None:
            return _cache["rows"]

        drive = _get_drive()

        q = f"'{_CS_REPORT_FOLDER_ID}' in parents and trashed = false"
        results = drive.files().list(
            q=q,
            fields="files(id, name, modifiedTime)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            corpora="drive",
            driveId=_DATA_EXPORTS_DRIVE_ID,
            pageSize=5,
            orderBy="modifiedTime desc",
        ).execute()
        files = results.get("files", [])
        if not files:
            logger.warning("No CS Report found in Data Exports drive")
            return []

        latest = files[0]
        logger.info("Fetching CS Report: %s (%s)", latest["name"], latest["modifiedTime"][:10])

        request = drive.files().export_media(
            fileId=latest["id"],
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        buf = io.BytesIO()
        from googleapiclient.http import MediaIoBaseDownload
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        buf.seek(0)
        import openpyxl
        wb = openpyxl.load_workbook(buf, read_only=True)
        ws = wb[wb.sheetnames[0]]

        headers: list[str] = []
        rows: list[dict[str, Any]] = []
        for ri, row in enumerate(ws.iter_rows()):
            cells = list(row)
            if ri == 0:
                headers = [str(c.value) if c.value else "" for c in cells]
                continue
            vals: dict[str, Any] = {}
            for i, c in enumerate(cells):
                if i < len(headers) and headers[i]:
                    vals[headers[i]] = c.value
            if vals.get("customer"):
                rows.append(vals)
        wb.close()

        _cache = {"rows": rows, "file": latest["name"], "modified": latest["modifiedTime"]}
        logger.info("Loaded %d rows from CS Report (%d columns)", len(rows), len(headers))
        return rows


def _customer_rows(customer_name: str, delta: str = "week") -> list[dict[str, Any]]:
    """Get rows for a customer filtered to a specific time delta."""
    rows = _fetch_latest_report()
    return [
        r for r in rows
        if r.get("customer", "").lower() == customer_name.lower()
        and r.get("delta") == delta
    ]


def _add_site_entity_from_row(row: dict[str, Any], entry: dict[str, Any]) -> None:
    """If the CS Report row has site/entity columns (any casing), add them to entry."""
    for key, val in row.items():
        if val is None or val == "":
            continue
        k = key.strip().lower()
        if k == "site":
            entry["site"] = val if isinstance(val, str) else str(val)
        elif k == "entity":
            entry["entity"] = val if isinstance(val, str) else str(val)


# ── Public API ──


def get_customer_platform_health(customer_name: str) -> dict[str, Any]:
    """Health scores, component availability, CTB/CTC, and shortage summary per site."""
    sites = _customer_rows(customer_name, "week")
    if not sites:
        return {"error": f"No CS Report data for '{customer_name}'", "source": "cs_report"}

    site_health: list[dict[str, Any]] = []
    total_shortages = 0
    total_critical = 0
    health_colors: dict[str, int] = {}

    for r in sites:
        factory = r.get("factoryName", "Unknown")
        health = r.get("healthScore", "NONE")
        health_colors[health] = health_colors.get(health, 0) + 1

        shortages = _kpi_end(r.get("shortageItemCount"))
        critical = _kpi_end(r.get("criticalShortages"))
        ctb = _kpi_end(r.get("clearToBuildPercent"))
        ctc = _kpi_end(r.get("clearToCommitPercent"))
        comp_avail = _kpi_end(r.get("componentAvailabilityPercent"))
        comp_proj = _kpi_end(r.get("componentAvailabilityPercentProjected"))
        buyer_qual = _kpi_end(r.get("buyerMappingQualityScore"))
        weekly_active = _kpi_end(r.get("weeklyActiveBuyersPercent"))
        risk_high = _kpi_end(r.get("aggregateRiskScoreHighCount"))

        if shortages:
            total_shortages += int(shortages)
        if critical:
            total_critical += int(critical)

        entry: dict[str, Any] = {"factory": factory, "health_score": health}
        if ctb is not None:
            entry["clear_to_build_pct"] = round(ctb, 1)
        if ctc is not None:
            entry["clear_to_commit_pct"] = round(ctc, 1)
        if comp_avail is not None:
            entry["component_availability_pct"] = round(comp_avail, 1)
        if comp_proj is not None:
            entry["component_availability_projected_pct"] = round(comp_proj, 1)
        if shortages is not None:
            entry["shortages"] = int(shortages)
        if critical is not None:
            entry["critical_shortages"] = int(critical)
        if weekly_active is not None:
            entry["weekly_active_buyers_pct"] = round(weekly_active, 1)
        if buyer_qual is not None:
            entry["buyer_mapping_quality"] = round(buyer_qual, 1)
        if risk_high is not None:
            entry["high_risk_items"] = int(risk_high)
        _add_site_entity_from_row(r, entry)

        site_health.append(entry)

    qa.check("CS Report platform health loaded")

    return {
        "customer": customer_name,
        "source": "cs_report",
        "factory_count": len(sites),
        "health_distribution": health_colors,
        "total_shortages": total_shortages,
        "total_critical_shortages": total_critical,
        "sites": sorted(site_health, key=lambda s: s.get("shortages", 0), reverse=True),
    }


def get_customer_supply_chain(customer_name: str) -> dict[str, Any]:
    """Inventory values, DOI, excess, and shortage trends per site."""
    sites = _customer_rows(customer_name, "week")
    if not sites:
        return {"error": f"No CS Report data for '{customer_name}'", "source": "cs_report"}

    site_data: list[dict[str, Any]] = []
    totals = {
        "on_hand": 0.0, "on_order": 0.0, "excess_on_hand": 0.0,
        "excess_on_order": 0.0, "past_due_po": 0.0, "past_due_req": 0.0,
    }

    for r in sites:
        factory = r.get("factoryName", "Unknown")
        on_hand = _kpi_end(r.get("totalOnHandValue"))
        on_order = _kpi_end(r.get("totalOnOrderValue"))
        excess_oh = _kpi_end(r.get("excessOnhandValuePositive"))
        excess_oo = _kpi_end(r.get("excessOnOrderValuePositive"))
        doi = _kpi_end(r.get("doiForwards"))
        days_cov = _kpi_end(r.get("daysCoverage"))
        past_po = _kpi_end(r.get("pastDuePOValue"))
        past_req = _kpi_end(r.get("pastDueRequirementValue"))
        late_po = _kpi_end(r.get("latePOCount"))
        late_pr = _kpi_end(r.get("latePRCount"))
        daily_usage = _kpi_end(r.get("dailyInventoryUsage"))
        toi = _kpi_end(r.get("toiForwards"))

        if on_hand:
            totals["on_hand"] += on_hand
        if on_order:
            totals["on_order"] += on_order
        if excess_oh:
            totals["excess_on_hand"] += excess_oh
        if excess_oo:
            totals["excess_on_order"] += excess_oo
        if past_po:
            totals["past_due_po"] += past_po
        if past_req:
            totals["past_due_req"] += past_req

        entry: dict[str, Any] = {"factory": factory}
        if on_hand is not None:
            entry["on_hand_value"] = round(on_hand)
        if on_order is not None:
            entry["on_order_value"] = round(on_order)
        if excess_oh is not None:
            entry["excess_on_hand"] = round(excess_oh)
        if doi is not None:
            entry["doi_days"] = round(doi, 1)
        if days_cov is not None:
            entry["days_coverage"] = round(days_cov, 1)
        if toi is not None:
            entry["turns_of_inventory"] = round(toi, 2)
        if late_po is not None:
            entry["late_pos"] = int(late_po)
        if late_pr is not None:
            entry["late_prs"] = int(late_pr)
        _add_site_entity_from_row(r, entry)

        site_data.append(entry)

    qa.check("CS Report supply chain loaded")

    return {
        "customer": customer_name,
        "source": "cs_report",
        "factory_count": len(sites),
        "totals": {k: round(v) for k, v in totals.items()},
        "sites": sorted(site_data, key=lambda s: s.get("on_hand_value", 0), reverse=True),
    }


def get_customer_platform_value(customer_name: str) -> dict[str, Any]:
    """ROI metrics: savings, open IA value, recs created, PO activity."""
    sites = _customer_rows(customer_name, "week")
    if not sites:
        return {"error": f"No CS Report data for '{customer_name}'", "source": "cs_report"}

    site_data: list[dict[str, Any]] = []
    total_savings = 0.0
    total_open_value = 0.0
    total_recs = 0
    total_pos_placed = 0
    total_overdue = 0
    total_potential_savings = 0.0
    total_potential_sell = 0.0

    for r in sites:
        factory = r.get("factoryName", "Unknown")
        savings = _kpi_end(r.get("inventoryActionCurrentReportingPeriodSavings"))
        open_val = _kpi_end(r.get("inventoryActionOpenValue"))
        recs = _kpi_end(r.get("recsCreatedLast30DaysCt"))
        pos = _kpi_end(r.get("posPlacedInLast30DaysCt"))
        overdue = _kpi_end(r.get("workbenchOverdueTasksCt"))
        pot_save = _kpi_end(r.get("potentialSavings"))
        pot_sell = _kpi_end(r.get("potentialToSell"))
        fy_spend = _kpi_end(r.get("currentFySpend"))
        prev_spend = _kpi_end(r.get("previousFySpend"))

        if savings:
            total_savings += savings
        if open_val:
            total_open_value += open_val
        if recs:
            total_recs += int(recs)
        if pos:
            total_pos_placed += int(pos)
        if overdue:
            total_overdue += int(overdue)
        if pot_save and pot_save > 0:
            total_potential_savings += pot_save
        if pot_sell and pot_sell > 0:
            total_potential_sell += pot_sell

        entry: dict[str, Any] = {"factory": factory}
        if savings is not None:
            entry["savings_current_period"] = round(savings)
        if open_val is not None:
            entry["open_ia_value"] = round(open_val)
        if recs is not None:
            entry["recs_created_30d"] = int(recs)
        if pos is not None:
            entry["pos_placed_30d"] = int(pos)
        if overdue is not None:
            entry["overdue_tasks"] = int(overdue)
        if fy_spend is not None:
            entry["current_fy_spend"] = round(fy_spend)
        if prev_spend is not None:
            entry["previous_fy_spend"] = round(prev_spend)
        _add_site_entity_from_row(r, entry)

        site_data.append(entry)

    qa.check("CS Report platform value loaded")

    return {
        "customer": customer_name,
        "source": "cs_report",
        "factory_count": len(sites),
        "total_savings": round(total_savings),
        "total_open_ia_value": round(total_open_value),
        "total_potential_savings": round(total_potential_savings),
        "total_potential_to_sell": round(total_potential_sell),
        "total_recs_created_30d": total_recs,
        "total_pos_placed_30d": total_pos_placed,
        "total_overdue_tasks": total_overdue,
        "sites": sorted(site_data, key=lambda s: s.get("savings_current_period", 0), reverse=True),
    }


def cross_validate_with_pendo(customer_name: str, pendo_health: dict[str, Any]) -> None:
    """Compare CS Report data with Pendo data and flag discrepancies via QA."""
    sites = _customer_rows(customer_name, "week")
    if not sites:
        return

    # 1. Site count: Pendo total_sites vs CS Report factory count
    pendo_site_count = pendo_health.get("account", {}).get("total_sites", 0)
    cs_factory_count = len(sites)
    if pendo_site_count == cs_factory_count:
        qa.check("Site count matches between Pendo and CS Report")
    else:
        qa.flag(
            f"Site count differs: Pendo sees {pendo_site_count} sites, CS Report has {cs_factory_count} factories",
            expected=pendo_site_count,
            actual=cs_factory_count,
            sources=("Pendo visitor data", "CS Report factory list"),
            severity="info",
            auto_corrected=False,
        )

    # 2. Engagement: Pendo weekly active rate vs CS Report weekly active buyers
    pendo_rate = pendo_health.get("engagement", {}).get("active_rate_7d", 0)
    cs_buyer_rates = []
    for r in sites:
        wab = _kpi_end(r.get("weeklyActiveBuyersPercent"))
        if wab is not None and wab > 0:
            cs_buyer_rates.append(wab)
    if cs_buyer_rates:
        cs_avg_rate = round(sum(cs_buyer_rates) / len(cs_buyer_rates), 1)
        diff = abs(pendo_rate - cs_avg_rate)
        if diff <= 15:
            qa.check("Pendo and CS Report engagement rates are consistent")
        else:
            qa.flag(
                f"Engagement rate gap: Pendo app login rate {pendo_rate}% vs CS Report buyer engagement {cs_avg_rate}%",
                expected=f"within 15pp",
                actual=f"{diff:.0f}pp difference",
                sources=("Pendo active_rate_7d", "CS Report weeklyActiveBuyersPercent"),
                severity="info" if diff <= 30 else "warning",
                auto_corrected=False,
            )

    # 3. Factory names: check if CS Report factories match Pendo sitenames
    pendo_sites = set()
    for s in pendo_health.get("sites", []):
        if isinstance(s, dict):
            pendo_sites.add(s.get("sitename", "").lower())
    cs_factories = set(r.get("factoryName", "").lower() for r in sites)
    prefix = customer_name.lower()
    pendo_stripped = set(
        s.replace(prefix, "").strip() for s in pendo_sites
    )
    cs_only = cs_factories - pendo_stripped - {""}
    pendo_only = pendo_stripped - cs_factories - {""}
    if cs_only or pendo_only:
        qa.flag(
            f"Site name mismatch: {len(cs_only)} in CS Report only, {len(pendo_only)} in Pendo only",
            expected="matching site lists",
            actual=f"CS-only: {sorted(cs_only)[:5]}, Pendo-only: {sorted(pendo_only)[:5]}",
            sources=("CS Report factoryName", "Pendo sitename"),
            severity="info",
        )
    else:
        qa.check("Factory names consistent between Pendo and CS Report")
