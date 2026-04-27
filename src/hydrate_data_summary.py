"""Data summary and prompt-budget helpers for hydrate/adapt."""

from __future__ import annotations

import json
from typing import Any

from .config import logger
from .cs_report_client import get_csr_section

ADAPT_PROMPT_DATA_MAX_CHARS = 12000
_ADAPT_OVERSIZE_WARN_EMITTED = False


def build_data_summary(report: dict[str, Any]) -> dict[str, Any]:
    """Compact summary of all available current data for GPT matching."""
    summary: dict[str, Any] = {
        "customer_name": report.get("customer", ""),
        "report_date": report.get("generated", ""),
        "quarter": report.get("quarter", ""),
        "quarter_start": report.get("quarter_start", ""),
        "quarter_end": report.get("quarter_end", ""),
    }

    acct = report.get("account", {})
    total_visitors = acct.get("total_visitors", 0)
    summary["total_users"] = total_visitors
    summary["total_visitors"] = total_visitors
    summary["unique_visitors"] = total_visitors
    summary["active_users"] = acct.get("active_visitors", 0)
    summary["total_sites"] = acct.get("total_sites", 0)
    summary["active_sites"] = acct.get("active_sites", 0)
    summary["health_score"] = acct.get("health_score", "")

    sites = report.get("sites", [])
    total_minutes = 0
    for site in sites:
        try:
            total_minutes += int(site.get("total_minutes") or 0)
        except (TypeError, ValueError):
            pass
    days = int(report.get("days") or 90)
    weeks = max(days / 7.0, 1e-6)
    summary["account_total_minutes"] = total_minutes
    summary["account_avg_weekly_hours"] = (
        round(total_minutes / 60.0 / weeks, 1) if total_minutes else 0.0
    )

    summary["site_details"] = [
        {
            "name": site.get("sitename", ""),
            "visitors": site.get("visitors", 0),
            "pages_used": site.get("pages_used", 0),
            "features_used": site.get("features_used", 0),
            "events": site.get("total_events", 0),
            "total_minutes": site.get("total_minutes", 0),
            "last_active": site.get("last_active", ""),
        }
        for site in sites[:30]
    ]

    csr = get_csr_section(report)
    cs = csr.get("platform_health") or {}
    if cs and not cs.get("error"):
        ts = cs.get("total_shortages")
        if ts is not None:
            summary["total_shortages"] = int(ts)
        tc = cs.get("total_critical_shortages")
        if tc is not None:
            summary["total_critical_shortages"] = int(tc)
        rates: list[float] = []
        for row in cs.get("sites") or []:
            weekly_active = row.get("weekly_active_buyers_pct")
            if weekly_active is not None:
                try:
                    rates.append(float(weekly_active))
                except (TypeError, ValueError):
                    pass
        if rates:
            summary["weekly_active_buyers_pct_avg"] = round(sum(rates) / len(rates), 1)
        summary["cs_health_sites"] = [
            {
                "site": row.get("site", ""),
                "health": row.get("health_status", ""),
                "ctb": row.get("ctb_pct", ""),
                "ctc": row.get("ctc_pct", ""),
            }
            for row in cs.get("sites", [])[:20]
        ]

    jira = report.get("jira", {}) or report.get("jira_summary", {})
    if jira:
        summary["support"] = {
            "total_tickets": jira.get("total_issues", 0),
            "open": jira.get("open_issues", 0),
            "resolved": jira.get("resolved_issues", 0),
        }

    sf = report.get("salesforce", {})
    if sf and isinstance(sf, dict) and "error" not in sf:
        summary["salesforce"] = {
            "accounts": sf.get("accounts", []),
            "opportunity_count_this_year": sf.get("opportunity_count_this_year", 0),
            "pipeline_arr": sf.get("pipeline_arr", 0),
        }

    platform_value = csr.get("platform_value") or {}
    if platform_value:
        summary["platform_value"] = platform_value

    supply_chain = csr.get("supply_chain") or {}
    if supply_chain:
        summary["supply_chain"] = supply_chain

    return summary


def prune_data_summary_for_prompt(
    data: dict[str, Any], *, site_limit: int, cs_limit: int, account_limit: int
) -> dict[str, Any]:
    """Return a shallow-deep copy with large list fields trimmed so prompts stay bounded."""
    out: dict[str, Any] = {}
    for key, value in data.items():
        if key == "site_details" and isinstance(value, list):
            out[key] = value[:site_limit]
        elif key == "cs_health_sites" and isinstance(value, list):
            out[key] = value[:cs_limit]
        elif key == "salesforce" and isinstance(value, dict):
            sf = dict(value)
            acct = sf.get("accounts")
            if isinstance(acct, list):
                sf["accounts"] = acct[:account_limit]
            out[key] = sf
        elif key in ("platform_value", "supply_chain") and isinstance(value, dict):
            out[key] = truncate_strings_in_obj(
                value, max_str=800, max_list_items=40, max_dict_keys=160
            )
        else:
            out[key] = value
    return out


def truncate_strings_in_obj(
    obj: Any,
    *,
    max_str: int,
    max_list_items: int,
    max_dict_keys: int | None = None,
) -> Any:
    """Recursively shorten long strings and cap list lengths for prompt size limits."""
    if isinstance(obj, str):
        return obj if len(obj) <= max_str else obj[: max_str - 1] + "…"
    if isinstance(obj, list):
        return [
            truncate_strings_in_obj(
                item,
                max_str=max_str,
                max_list_items=max_list_items,
                max_dict_keys=max_dict_keys,
            )
            for item in obj[:max_list_items]
        ]
    if isinstance(obj, dict):
        items = sorted(obj.items(), key=lambda kv: str(kv[0]))
        if max_dict_keys is not None and len(items) > max_dict_keys:
            items = items[:max_dict_keys]
        return {
            key: truncate_strings_in_obj(
                value,
                max_str=max_str,
                max_list_items=max_list_items,
                max_dict_keys=max_dict_keys,
            )
            for key, value in items
        }
    return obj


def format_data_summary_for_adapt_prompt(data_summary: dict[str, Any]) -> str:
    """Serialize data_summary for the adapt LLM using bounded compact JSON."""
    global _ADAPT_OVERSIZE_WARN_EMITTED
    max_chars = ADAPT_PROMPT_DATA_MAX_CHARS
    site_tiers = [(30, 20, 25), (20, 15, 15), (15, 10, 10), (10, 8, 8), (8, 5, 5), (5, 3, 3)]
    truncate_tiers = [(600, 50, 128), (400, 35, 96), (300, 25, 72), (200, 16, 56), (120, 10, 40)]
    for site_l, cs_l, acct_l in site_tiers:
        pruned = prune_data_summary_for_prompt(
            data_summary, site_limit=site_l, cs_limit=cs_l, account_limit=acct_l
        )
        for max_str, max_list, max_dk in truncate_tiers:
            compact = json.dumps(
                truncate_strings_in_obj(
                    pruned,
                    max_str=max_str,
                    max_list_items=max_list,
                    max_dict_keys=max_dk,
                ),
                separators=(",", ":"),
                sort_keys=True,
                default=str,
            )
            if len(compact) <= max_chars:
                if (site_l, cs_l, acct_l) != (30, 20, 25) or (max_str, max_list, max_dk) != (
                    600,
                    50,
                    128,
                ):
                    logger.info(
                        "hydrate: adapt prompt data_summary pruned to fit (%d chars, "
                        "site=%d cs=%d acct=%d, str=%d list=%d dict_keys=%d)",
                        len(compact),
                        site_l,
                        cs_l,
                        acct_l,
                        max_str,
                        max_list,
                        max_dk,
                    )
                return compact
    minimal = prune_data_summary_for_prompt(data_summary, site_limit=3, cs_limit=2, account_limit=2)
    for max_str, max_list, max_dk in [(300, 20, 48), (200, 12, 32), (120, 8, 24), (80, 5, 16)]:
        compact = json.dumps(
            truncate_strings_in_obj(
                minimal,
                max_str=max_str,
                max_list_items=max_list,
                max_dict_keys=max_dk,
            ),
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
        if len(compact) <= max_chars:
            logger.info(
                "hydrate: adapt prompt data_summary aggressive truncation fit (%d chars, str=%d list=%d dict_keys=%d)",
                len(compact),
                max_str,
                max_list,
                max_dk,
            )
            return compact
    compact = json.dumps(
        truncate_strings_in_obj(
            minimal,
            max_str=60,
            max_list_items=4,
            max_dict_keys=12,
        ),
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    if len(compact) > max_chars:
        if not _ADAPT_OVERSIZE_WARN_EMITTED:
            logger.warning(
                "hydrate: data_summary still oversized after pruning; truncating JSON to %d chars",
                max_chars,
            )
            _ADAPT_OVERSIZE_WARN_EMITTED = True
        else:
            logger.debug(
                "hydrate: data_summary still oversized after pruning; truncating JSON to %d chars (repeat)",
                max_chars,
            )
        return compact[: max_chars - 1] + "…"
    return compact


def reset_for_tests() -> None:
    global _ADAPT_OVERSIZE_WARN_EMITTED
    _ADAPT_OVERSIZE_WARN_EMITTED = False
