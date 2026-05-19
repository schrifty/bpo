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
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

from .config import logger
from .qa import qa

# Shared Drive ID and folder for the CS Report
_DATA_EXPORTS_DRIVE_ID = "0AHL7kClvi-JmUk9PVA"
_CS_REPORT_FOLDER_ID = "16922c1MWTKNx3Pw1W7bbUARC_q2aat0Z"

_cache: dict[str, Any] | None = None
_cache_lock = threading.Lock()

# Optional: project-root YAML — map Pendo customer → exact CS Report `customer` values
_CSR_ALIAS_FILE = Path(__file__).resolve().parent.parent / "cs_report_customer_aliases.yaml"
_COHORTS_FILE = Path(__file__).resolve().parent.parent / "cohorts.yaml"
_cs_report_alias_map: dict[str, list[str]] | None = None
_cs_report_alias_lock = threading.Lock()
_cohort_customer_alias_map: dict[str, list[str]] | None = None
_cohort_customer_alias_lock = threading.Lock()


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


def _normalize_health_score(raw: Any) -> str:
    """CSR health bucket (GREEN/YELLOW/RED/NONE); accepts plain strings or JSON KPI cells."""
    if raw is None:
        return "NONE"
    if isinstance(raw, (int, float)):
        return "NONE"
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return "NONE"
        if s.startswith("{"):
            d = _parse_kpi(s)
            if d:
                v = d.get("endValue")
                if v is None:
                    v = d.get("startValue")
                if v is not None:
                    s = str(v).strip()
        upper = s.upper()
        if upper in ("GREEN", "YELLOW", "RED", "NONE"):
            return upper
        return s
    return "NONE"


def _health_score_from_row(row: dict[str, Any]) -> str:
    for col in ("healthScore", "health_score", "Health Score"):
        if col in row:
            return _normalize_health_score(row.get(col))
    for k, v in row.items():
        if str(k).strip().lower() == "healthscore":
            return _normalize_health_score(v)
    return "NONE"


def _csr_row_dedupe_key(row: dict[str, Any]) -> str:
    parts = [
        (row.get("customer") or "").strip().lower(),
        (row.get("factoryName") or "").strip().lower(),
        (row.get("entity") or row.get("Entity") or "").strip().lower(),
        (row.get("site") or row.get("Site") or "").strip().lower(),
    ]
    return "|".join(parts)


def _kpi_delta_pct(raw) -> float | None:
    """Extract the delta percentage from a KPI cell."""
    d = _parse_kpi(raw)
    if d is None:
        return None
    v = d.get("deltaPercent")
    return float(v) if v is not None else None


def check_reachable() -> None:
    """Verify the CS Report folder on Drive is reachable. Raises if Drive or folder is down."""
    from .network_utils import network_timeout
    
    with network_timeout(30.0, "Drive CS Report folder check"):
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

        from .network_utils import network_timeout
        with network_timeout(30.0, "Drive CS Report download"):
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
            chunk_count = 0
            while not done:
                _, done = downloader.next_chunk()
                chunk_count += 1
                if chunk_count > 100:  # Safety limit: max 100 chunks
                    raise TimeoutError(f"CS Report download exceeded max chunks (100)")

        buf.seek(0)
        try:
            import openpyxl
        except ImportError as e:
            raise ImportError(
                "CS Report XLSX parsing requires openpyxl; add it to your environment "
                "(e.g. pip install openpyxl or pip install -r requirements.txt)."
            ) from e
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


def _load_cs_report_alias_map() -> dict[str, list[str]]:
    """Pendo (or QBR) customer name (lower) → list of exact CS `customer` column values."""
    global _cs_report_alias_map
    if _cs_report_alias_map is not None:
        return _cs_report_alias_map
    with _cs_report_alias_lock:
        if _cs_report_alias_map is not None:
            return _cs_report_alias_map
        out: dict[str, list[str]] = {}
        if _CSR_ALIAS_FILE.is_file():
            try:
                raw = yaml.safe_load(_CSR_ALIAS_FILE.read_text())
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        if str(k).strip().startswith("#"):
                            continue
                        key = str(k).strip().lower()
                        if not key:
                            continue
                        if isinstance(v, str):
                            vals = [v]
                        elif isinstance(v, list):
                            vals = [str(x).strip() for x in v if str(x).strip()]
                        else:
                            continue
                        out[key] = vals
            except Exception as e:
                logger.warning("cs_report_customer_aliases: could not load %s: %s", _CSR_ALIAS_FILE, e)
        _cs_report_alias_map = out
        return out


def _load_cohort_customer_alias_map() -> dict[str, list[str]]:
    """Map cohort key / name / alias → all related account labels (for CSR lookup expansion)."""
    global _cohort_customer_alias_map
    if _cohort_customer_alias_map is not None:
        return _cohort_customer_alias_map
    with _cohort_customer_alias_lock:
        if _cohort_customer_alias_map is not None:
            return _cohort_customer_alias_map
        out: dict[str, list[str]] = {}
        if _COHORTS_FILE.is_file():
            try:
                data = yaml.safe_load(_COHORTS_FILE.read_text()) or {}
            except Exception as e:
                logger.warning("cohorts aliases (CS Report): could not load %s: %s", _COHORTS_FILE, e)
                data = {}
            customers = data.get("customers") if isinstance(data, dict) else None
            if not isinstance(customers, dict) and isinstance(data, dict):
                customers = data.get("cohorts")
            if isinstance(customers, dict):
                for key, row in customers.items():
                    terms: list[str] = [str(key).strip()]
                    if isinstance(row, dict):
                        name = str(row.get("name") or "").strip()
                        if name:
                            terms.append(name)
                        aliases = row.get("aliases") or []
                        if isinstance(aliases, str):
                            aliases = [aliases]
                        if isinstance(aliases, (list, tuple)):
                            terms.extend(str(a).strip() for a in aliases if str(a).strip())
                    deduped: list[str] = []
                    seen: set[str] = set()
                    for t in terms:
                        if t and t.lower() not in seen:
                            seen.add(t.lower())
                            deduped.append(t)
                    for t in deduped:
                        out[t.lower()] = deduped
        _cohort_customer_alias_map = out
        return out


def cs_report_lookup_keys_for_account(
    *,
    salesforce_label: str = "",
    pendo_customer_key: str | None = None,
) -> list[str]:
    """Ordered lookup keys for CS Report row matching (SF label, Pendo prefix, cohort aliases)."""
    keys: list[str] = []

    def add(term: str) -> None:
        s = (term or "").strip()
        if not s:
            return
        if s.lower() in {k.lower() for k in keys}:
            return
        keys.append(s)

    add(salesforce_label)
    add(pendo_customer_key or "")
    cohort = _load_cohort_customer_alias_map()
    for seed in list(keys):
        for term in cohort.get(seed.lower(), []):
            add(term)
    return keys


def cs_report_customer_name_candidates(pendo_name: str) -> list[str]:
    """Return distinct names to try for CS `customer` matching: pendo name first, then aliases."""
    raw = (pendo_name or "").strip()
    if not raw:
        return []
    al = _load_cs_report_alias_map().get(raw.lower()) or []
    out: list[str] = []
    seen: set[str] = set()
    for c in (raw, *al):
        s = (c or "").strip()
        if not s:
            continue
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def _customer_rows(customer_name: str, delta: str = "week") -> list[dict[str, Any]]:
    """Get rows for one lookup key (plus ``cs_report_customer_aliases.yaml`` candidates)."""
    sites, _matched, _tried, _merged = _sites_for_customer_lookup(customer_name, delta=delta)
    return sites


def _sites_for_customer_lookup(
    primary_name: str,
    *,
    lookup_keys: list[str] | None = None,
    delta: str = "week",
) -> tuple[list[dict[str, Any]], str | None, list[str], list[str]]:
    """Merge week rows for every CSR ``customer`` resolved from *lookup_keys* and aliases.

    Returns ``(rows, matched_lookup_key, tried_customer_values, matched_csr_customers)``.
    """
    rows = _fetch_latest_report()
    keys: list[str] = []
    for k in lookup_keys or []:
        s = (k or "").strip()
        if s and s.lower() not in {x.lower() for x in keys}:
            keys.append(s)
    primary = (primary_name or "").strip()
    if primary and primary.lower() not in {x.lower() for x in keys}:
        keys.insert(0, primary)
    if not keys and primary:
        keys = [primary]

    all_tried: list[str] = []
    seen_tried: set[str] = set()
    merged: list[dict[str, Any]] = []
    seen_row_keys: set[str] = set()
    matched_csr_customers: list[str] = []
    matched_lookup_key: str | None = None

    for key in keys:
        cands = cs_report_customer_name_candidates(key)
        key_lower = key.lower()
        for name in cands:
            nl = name.lower()
            if nl in seen_tried:
                continue
            seen_tried.add(nl)
            all_tried.append(name)
            matched = [
                r
                for r in rows
                if (r.get("customer") or "").strip().lower() == nl and r.get("delta") == delta
            ]
            if not matched:
                continue
            if matched_lookup_key is None:
                matched_lookup_key = key
            if name not in matched_csr_customers:
                matched_csr_customers.append(name)
            if nl != key_lower:
                logger.info(
                    "CS Report: matched %d row(s) for lookup key %r using `customer`=%r",
                    len(matched),
                    key,
                    name,
                )
            for r in matched:
                rk = _csr_row_dedupe_key(r)
                if rk in seen_row_keys:
                    continue
                seen_row_keys.add(rk)
                merged.append(r)

    if merged and len(matched_csr_customers) > 1:
        logger.info(
            "CS Report: merged %d row(s) across CSR customer values %r for account %r",
            len(merged),
            matched_csr_customers,
            primary_name or matched_lookup_key,
        )
    return merged, matched_lookup_key, all_tried, matched_csr_customers


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


def get_customer_platform_health(
    customer_name: str,
    *,
    lookup_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Health scores, component availability, CTB/CTC, and shortage summary per site."""
    sites, matched_key, tried, matched_csr_customers = _sites_for_customer_lookup(
        customer_name, lookup_keys=lookup_keys, delta="week"
    )
    if not sites:
        return {
            "error": (
                f"No CS Report data for {customer_name!r} "
                f"(lookup_keys={lookup_keys!r}, tried `customer`={tried!r}, delta=week)"
            ),
            "source": "cs_report",
        }
    display_name = matched_key or customer_name

    site_health: list[dict[str, Any]] = []
    total_shortages = 0
    total_critical = 0
    health_colors: dict[str, int] = {}

    for r in sites:
        factory = r.get("factoryName", "Unknown")
        health = _health_score_from_row(r)
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
        "customer": display_name,
        "source": "cs_report",
        "factory_count": len(sites),
        "csr_customer_names_merged": matched_csr_customers,
        "health_distribution": health_colors,
        "total_shortages": total_shortages,
        "total_critical_shortages": total_critical,
        "sites_sort": "shortages_desc",
        "sites_note": (
            "Per-factory list is sorted by shortages (highest first). High-shortage sites often "
            "show NONE/RED health; use health_distribution for the full account mix."
        ),
        "sites": sorted(site_health, key=lambda s: s.get("shortages", 0), reverse=True),
    }


def get_customer_supply_chain(
    customer_name: str,
    *,
    lookup_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Inventory values, DOI, excess, and shortage trends per site."""
    sites, matched_key, tried, _matched_csr = _sites_for_customer_lookup(
        customer_name, lookup_keys=lookup_keys, delta="week"
    )
    if not sites:
        return {
            "error": (
                f"No CS Report data for {customer_name!r} "
                f"(lookup_keys={lookup_keys!r}, tried `customer`={tried!r}, delta=week)"
            ),
            "source": "cs_report",
        }
    display_name = matched_key or customer_name

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
        "customer": display_name,
        "source": "cs_report",
        "factory_count": len(sites),
        "totals": {k: round(v) for k, v in totals.items()},
        "sites": sorted(site_data, key=lambda s: s.get("on_hand_value", 0), reverse=True),
    }


def get_customer_platform_value(
    customer_name: str,
    *,
    lookup_keys: list[str] | None = None,
) -> dict[str, Any]:
    """ROI metrics: savings, open IA value, recs created, PO activity."""
    sites, matched_key, tried, _matched_csr = _sites_for_customer_lookup(
        customer_name, lookup_keys=lookup_keys, delta="week"
    )
    if not sites:
        return {
            "error": (
                f"No CS Report data for {customer_name!r} "
                f"(lookup_keys={lookup_keys!r}, tried `customer`={tried!r}, delta=week)"
            ),
            "source": "cs_report",
        }
    display_name = matched_key or customer_name

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
        "customer": display_name,
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


def load_csr_all_customers_week() -> dict[str, Any]:
    """Build CSR-shaped aggregates by merging ``delta=week`` CS Report rows for every distinct ``customer``.

    Parses the latest XLSX once via :func:`_fetch_latest_report`, then reuses
    :func:`get_customer_platform_health`, :func:`get_customer_supply_chain`, and
    :func:`get_customer_platform_value` per customer. Site rows include ``csr_customer`` for provenance.
    """
    rows = _fetch_latest_report()
    customers = sorted(
        {
            (r.get("customer") or "").strip()
            for r in rows
            if r.get("delta") == "week" and (r.get("customer") or "").strip()
        }
    )
    err: dict[str, Any] = {"error": "No CS Report rows with delta=week", "source": "cs_report"}
    if not customers:
        return {"platform_health": dict(err), "supply_chain": dict(err), "platform_value": dict(err)}

    ph_sites: list[dict[str, Any]] = []
    health_distribution: dict[str, int] = {}
    total_shortages = 0
    total_critical_shortages = 0
    ph_factory_count = 0

    sc_sites: list[dict[str, Any]] = []
    sc_totals: dict[str, float] = defaultdict(float)
    sc_factory_count = 0

    pv_sites: list[dict[str, Any]] = []
    pv_factory_count = 0
    total_savings = 0.0
    total_open_ia_value = 0.0
    total_potential_savings = 0.0
    total_potential_to_sell = 0.0
    total_recs = 0
    total_pos = 0
    total_overdue = 0

    for cn in customers:
        ph = get_customer_platform_health(cn)
        if not ph.get("error"):
            ph_factory_count += int(ph.get("factory_count") or 0)
            total_shortages += int(ph.get("total_shortages") or 0)
            total_critical_shortages += int(ph.get("total_critical_shortages") or 0)
            for hk, hv in (ph.get("health_distribution") or {}).items():
                health_distribution[hk] = health_distribution.get(hk, 0) + int(hv)
            for s in ph.get("sites") or []:
                row = dict(s)
                row["csr_customer"] = cn
                ph_sites.append(row)

        sc = get_customer_supply_chain(cn)
        if not sc.get("error"):
            sc_factory_count += int(sc.get("factory_count") or 0)
            for k, v in (sc.get("totals") or {}).items():
                if isinstance(v, (int, float)):
                    sc_totals[str(k)] += float(v)
            for s in sc.get("sites") or []:
                row = dict(s)
                row["csr_customer"] = cn
                sc_sites.append(row)

        pv = get_customer_platform_value(cn)
        if not pv.get("error"):
            pv_factory_count += int(pv.get("factory_count") or 0)
            total_savings += float(pv.get("total_savings") or 0)
            total_open_ia_value += float(pv.get("total_open_ia_value") or 0)
            total_potential_savings += float(pv.get("total_potential_savings") or 0)
            total_potential_to_sell += float(pv.get("total_potential_to_sell") or 0)
            total_recs += int(pv.get("total_recs_created_30d") or 0)
            total_pos += int(pv.get("total_pos_placed_30d") or 0)
            total_overdue += int(pv.get("total_overdue_tasks") or 0)
            for s in pv.get("sites") or []:
                row = dict(s)
                row["csr_customer"] = cn
                pv_sites.append(row)

    label = "All Customers (CS Report aggregate)"
    merged_ph: dict[str, Any] = {
        "customer": label,
        "source": "cs_report",
        "aggregate_scope": "all_customers_week",
        "distinct_csr_customers": len(customers),
        "factory_count": ph_factory_count,
        "health_distribution": health_distribution,
        "total_shortages": total_shortages,
        "total_critical_shortages": total_critical_shortages,
        "sites": sorted(ph_sites, key=lambda s: s.get("shortages", 0), reverse=True),
    }
    merged_sc: dict[str, Any] = {
        "customer": label,
        "source": "cs_report",
        "aggregate_scope": "all_customers_week",
        "distinct_csr_customers": len(customers),
        "factory_count": sc_factory_count,
        "totals": {k: round(v) for k, v in sc_totals.items()},
        "sites": sorted(sc_sites, key=lambda s: s.get("on_hand_value", 0), reverse=True),
    }
    merged_pv: dict[str, Any] = {
        "customer": label,
        "source": "cs_report",
        "aggregate_scope": "all_customers_week",
        "distinct_csr_customers": len(customers),
        "factory_count": pv_factory_count,
        "total_savings": round(total_savings),
        "total_open_ia_value": round(total_open_ia_value),
        "total_potential_savings": round(total_potential_savings),
        "total_potential_to_sell": round(total_potential_to_sell),
        "total_recs_created_30d": total_recs,
        "total_pos_placed_30d": total_pos,
        "total_overdue_tasks": total_overdue,
        "sites": sorted(pv_sites, key=lambda s: s.get("savings_current_period", 0), reverse=True),
    }
    return {"platform_health": merged_ph, "supply_chain": merged_sc, "platform_value": merged_pv}


def load_csr_top_customers_by_arr(
    selection: list[dict[str, Any]],
) -> dict[str, Any]:
    """Load per-customer CS Report week slices for a ranked ARR selection (LLM export §4).

    *selection* rows should include ``salesforce_label``, ``arr``, and ``csr_lookup_name``
    (Pendo prefix or Salesforce label used for :func:`get_customer_platform_health` alias resolution).
    """
    if not selection:
        err: dict[str, Any] = {"error": "empty selection", "source": "cs_report"}
        return {
            "scope": "top_customers_by_arr",
            "top_n": 0,
            "selection_ranked": [],
            "customers": {},
            "platform_health": dict(err),
            "supply_chain": dict(err),
            "platform_value": dict(err),
        }

    customers: dict[str, Any] = {}
    selection_ranked: list[dict[str, Any]] = []

    for row in selection:
        if not isinstance(row, dict):
            continue
        sf_label = str(row.get("salesforce_label") or row.get("customer") or "").strip()
        if not sf_label:
            continue
        lookup_keys = cs_report_lookup_keys_for_account(
            salesforce_label=sf_label,
            pendo_customer_key=row.get("pendo_customer_key"),
        )
        ph = get_customer_platform_health(sf_label, lookup_keys=lookup_keys)
        sc = get_customer_supply_chain(sf_label, lookup_keys=lookup_keys)
        pv = get_customer_platform_value(sf_label, lookup_keys=lookup_keys)
        matched = None
        merged_csr_names: list[str] = []
        if isinstance(ph, dict) and not ph.get("error"):
            matched = ph.get("customer")
            raw_merged = ph.get("csr_customer_names_merged")
            if isinstance(raw_merged, list):
                merged_csr_names = [str(x) for x in raw_merged if str(x).strip()]
        customers[sf_label] = {
            "salesforce_label": sf_label,
            "arr": row.get("arr"),
            "pendo_customer_key": row.get("pendo_customer_key"),
            "csr_lookup_keys": lookup_keys,
            "csr_matched_lookup_key": matched,
            "csr_customer_names_merged": merged_csr_names,
            "csr_lookup_name": matched or (lookup_keys[0] if lookup_keys else sf_label),
            "platform_health": ph,
            "supply_chain": sc,
            "platform_value": pv,
        }
        selection_ranked.append(
            {
                "salesforce_label": sf_label,
                "arr": row.get("arr"),
                "csr_lookup_keys": lookup_keys,
                "csr_matched_lookup_key": matched,
                "csr_loaded": not all(
                    isinstance(block, dict) and block.get("error")
                    for block in (ph, sc, pv)
                ),
            }
        )

    return {
        "scope": "top_customers_by_arr",
        "top_n": len(selection_ranked),
        "aggregate_scope": "top_customers_by_arr",
        "note": (
            "Per-customer CS Report (delta=week) for the highest-ARR active Salesforce Customer Entity "
            "labels. Each entry under ``customers`` has platform_health, supply_chain, and "
            "platform_value for that account (not a portfolio-wide merge)."
        ),
        "selection_ranked": selection_ranked,
        "customers": customers,
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


def get_csr_section(report: dict[str, Any]) -> dict[str, Any]:
    """Return the CS Report (CSR) subsection from a merged health report.

    Preferred shape: ``report["csr"]`` with keys ``platform_health``, ``supply_chain``,
    ``platform_value``. Legacy top-level ``cs_platform_*`` keys are still supported.
    """
    csr = report.get("csr")
    if isinstance(csr, dict) and csr:
        return csr
    return {
        "platform_health": report.get("cs_platform_health") or {},
        "supply_chain": report.get("cs_supply_chain") or {},
        "platform_value": report.get("cs_platform_value") or {},
    }
