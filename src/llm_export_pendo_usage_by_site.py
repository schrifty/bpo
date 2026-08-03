"""Pendo usage-by-site attachment for the all-customers LLM export."""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Any

from .config import logger

# Active sites only keeps the payload useful and small (~600–700 rows for 90d).
_DEFAULT_ACTIVE_ONLY = True


def llm_export_usage_by_site_enabled() -> bool:
    raw = (os.environ.get("CORTEX_LLM_EXPORT_USAGE_BY_SITE") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _customer_rollups(sites: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "sites": 0,
            "page_views": 0,
            "feature_clicks": 0,
            "total_events": 0,
            "total_minutes": 0,
        }
    )
    for row in sites:
        if not isinstance(row, dict):
            continue
        customer = str(row.get("customer") or "").strip() or "(unknown)"
        agg = by[customer]
        agg["sites"] += 1
        for key in ("page_views", "feature_clicks", "total_events", "total_minutes"):
            try:
                agg[key] += int(row.get(key) or 0)
            except (TypeError, ValueError):
                pass
    out = [{"customer": customer, **metrics} for customer, metrics in by.items()]
    out.sort(key=lambda r: (-int(r.get("total_events") or 0), str(r.get("customer") or "")))
    return out


def attach_pendo_usage_by_site_for_llm_export(
    report: dict[str, Any],
    pc: Any,
    *,
    days: int,
) -> dict[str, Any]:
    """Set ``report['pendo_usage_by_site']`` from Pendo site usage aggregation."""
    summary: dict[str, Any] = {
        "enabled": llm_export_usage_by_site_enabled(),
        "days": int(days),
        "active_only": _DEFAULT_ACTIVE_ONLY,
        "sites_total": 0,
        "customers_with_sites": 0,
    }
    if not summary["enabled"]:
        report["pendo_usage_by_site"] = {
            "skipped": "CORTEX_LLM_EXPORT_USAGE_BY_SITE disabled",
            "days": int(days),
        }
        report["_llm_export_pendo_usage_by_site"] = summary
        logger.info("LLM export Pendo usage-by-site: skipped (disabled)")
        return summary

    try:
        raw = pc.get_all_sites_usage_report(days=int(days), active_only=_DEFAULT_ACTIVE_ONLY)
    except Exception as exc:
        err = str(exc)[:400]
        report["pendo_usage_by_site"] = {
            "error": err,
            "days": int(days),
            "active_only": _DEFAULT_ACTIVE_ONLY,
        }
        summary["error"] = err
        report["_llm_export_pendo_usage_by_site"] = summary
        logger.warning("LLM export Pendo usage-by-site: failed: %s", err)
        return summary

    sites_raw = raw.get("results") if isinstance(raw, dict) else None
    sites: list[dict[str, Any]] = []
    for row in sites_raw or []:
        if not isinstance(row, dict):
            continue
        sites.append(
            {
                "sitename": row.get("sitename"),
                "siteid": row.get("siteid"),
                "customer": row.get("customer"),
                "page_views": row.get("page_views"),
                "feature_clicks": row.get("feature_clicks"),
                "total_events": row.get("total_events"),
                "total_minutes": row.get("total_minutes"),
            }
        )

    by_customer = _customer_rollups(sites)
    summary["sites_total"] = len(sites)
    summary["customers_with_sites"] = len(by_customer)
    report["pendo_usage_by_site"] = {
        "source": "pendo",
        "days": int(days),
        "active_only": _DEFAULT_ACTIVE_ONLY,
        "note": (
            "Active Pendo sites only (total_events > 0 in the lookback). "
            "``sites`` is ranked by total_events; ``by_customer`` rolls the same rows up by "
            "sitename customer prefix. Per-site visitor uniqueness does not sum across sites."
        ),
        "sites_total": len(sites),
        "sites": sites,
        "by_customer": by_customer,
    }
    report["_llm_export_pendo_usage_by_site"] = summary
    logger.info(
        "LLM export Pendo usage-by-site: days=%d active_sites=%d customers=%d",
        days,
        len(sites),
        len(by_customer),
    )
    return summary


def compact_pendo_usage_by_site(
    payload: dict[str, Any] | None,
    *,
    size_caps_enabled: bool = True,
    site_limit: int | None = None,
) -> dict[str, Any]:
    """LLM-export shape for ``usage_by_site``.

    Active-site payloads are small (~30K tokens for a full portfolio), so the default is
    to keep every row. Pass ``site_limit`` only when a shrink tier must reclaim budget.
    """
    del size_caps_enabled  # reserved for future tighten tiers; default is full active set
    if not isinstance(payload, dict) or not payload:
        return {
            "note": "Pendo usage-by-site was not attached for this run.",
        }
    if payload.get("skipped"):
        return {"skipped": payload.get("skipped"), "days": payload.get("days")}
    if payload.get("error"):
        return {"error": payload.get("error"), "days": payload.get("days")}

    sites = payload.get("sites") if isinstance(payload.get("sites"), list) else []
    included = list(sites)
    truncated = False
    if site_limit is not None and int(site_limit) > 0 and len(included) > int(site_limit):
        included = included[: int(site_limit)]
        truncated = True

    out: dict[str, Any] = {
        "source": payload.get("source"),
        "days": payload.get("days"),
        "active_only": payload.get("active_only"),
        "note": payload.get("note"),
        "sites_total": payload.get("sites_total", len(sites)),
        "sites_included": len(included),
        "sites": included,
        "by_customer": payload.get("by_customer") if isinstance(payload.get("by_customer"), list) else [],
    }
    if truncated:
        out["sites_truncated"] = True
        out["note"] = (
            f"{out.get('note') or ''} Showing top {len(included)} of "
            f"{out.get('sites_total')} active sites by total_events."
        ).strip()
    return out
