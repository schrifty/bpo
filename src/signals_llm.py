"""Optional LLM rewrite of Notable Signals (Phase 2) — grounded in heuristic + cross-source lines.

Enabled with ``BPO_SIGNALS_LLM=1``. Runs after ``extend_health_report_signals`` on the full health report.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from typing import Any

from .config import (
    BPO_SIGNALS_LLM,
    BPO_SIGNALS_LLM_MAX_ITEMS,
    LLM_MODEL,
    llm_client,
    logger,
)
from .evaluate import _llm_create_with_retry, _strip_json_code_fence

# Compact payload for the model (avoid sending full site lists).
_PAYLOAD_MAX_HEURISTIC = 24
_PAYLOAD_MAX_STR = 500


def _trim_str(s: Any, n: int = _PAYLOAD_MAX_STR) -> str:
    if s is None:
        return ""
    t = str(s).strip()
    return t if len(t) <= n else t[: n - 1] + "…"


def _days_until_contract_end(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        end = _dt.datetime.strptime(str(raw).strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    return (end - _dt.date.today()).days


def _renewal_min_days(sf: dict[str, Any]) -> int | None:
    if not sf or not sf.get("matched"):
        return None
    best: int | None = None
    for a in sf.get("accounts") or []:
        du = _days_until_contract_end(a.get("Contract_Contract_End_Date__c"))
        if du is not None and 0 < du <= 180:
            if best is None or du < best:
                best = du
    return best


def build_signals_llm_payload(report: dict[str, Any]) -> dict[str, Any]:
    """Structured facts the model may use; heuristic lines are the source of truth for themes."""
    heur = list(report.get("signals") or [])[:_PAYLOAD_MAX_HEURISTIC]
    eng = report.get("engagement") or {}
    bench = report.get("benchmarks") or {}
    acct = report.get("account") or {}

    depth = report.get("depth") or {}
    exports = report.get("exports") or {}
    kei = report.get("kei") or {}

    jira = report.get("jira") or {}
    jira_sum: dict[str, Any] = {}
    if jira and not jira.get("error"):
        jira_sum = {
            "open_issues": jira.get("open_issues"),
            "resolved_issues": jira.get("resolved_issues"),
            "escalated": jira.get("escalated"),
            "open_bugs": jira.get("open_bugs"),
            "days": jira.get("days"),
        }

    sf = report.get("salesforce") or {}
    sf_sum: dict[str, Any] = {}
    if sf.get("matched"):
        arr = 0.0
        for a in sf.get("accounts") or []:
            try:
                arr += float(a.get("ARR__c") or 0)
            except (TypeError, ValueError):
                pass
        sf_sum = {
            "pipeline_arr": sf.get("pipeline_arr"),
            "opportunity_count_this_year": sf.get("opportunity_count_this_year"),
            "total_arr_on_accounts": round(arr, 2),
            "renewal_within_days_min": _renewal_min_days(sf),
        }

    ph = report.get("cs_platform_health") or {}
    cs_ph: dict[str, Any] = {}
    if ph and not ph.get("error"):
        cs_ph = {
            "health_distribution": ph.get("health_distribution"),
            "total_critical_shortages": ph.get("total_critical_shortages"),
            "total_shortages": ph.get("total_shortages"),
            "factory_count": ph.get("factory_count"),
        }

    sc = report.get("cs_supply_chain") or {}
    cs_sc: dict[str, Any] = {}
    if sc and not sc.get("error"):
        cs_sc = {"totals": sc.get("totals"), "factory_count": sc.get("factory_count")}

    pv = report.get("cs_platform_value") or {}
    cs_pv: dict[str, Any] = {}
    if pv and not pv.get("error"):
        cs_pv = {
            "total_savings": pv.get("total_savings"),
            "total_open_ia_value": pv.get("total_open_ia_value"),
            "total_overdue_tasks": pv.get("total_overdue_tasks"),
            "total_recs_created_30d": pv.get("total_recs_created_30d"),
        }

    insights = report.get("feature_adoption_insights")
    feat_narr = None
    if isinstance(insights, dict) and insights.get("narrative"):
        feat_narr = _trim_str(insights.get("narrative"), 400)

    champions_n = len(report.get("champions") or [])
    at_risk_n = len(report.get("at_risk_users") or [])

    return {
        "customer": report.get("customer", ""),
        "quarter": report.get("quarter", ""),
        "days": report.get("days"),
        "heuristic_signals": heur,
        "engagement": {
            "active_7d": eng.get("active_7d"),
            "dormant": eng.get("dormant"),
            "active_rate_7d_pct": eng.get("active_rate_7d"),
        },
        "benchmarks": {
            "customer_active_rate": bench.get("customer_active_rate"),
            "peer_median_rate": bench.get("peer_median_rate"),
            "cohort_name": bench.get("cohort_name"),
            "cohort_median_rate": bench.get("cohort_median_rate"),
        },
        "account": {
            "total_visitors": acct.get("total_visitors"),
            "total_sites": acct.get("total_sites"),
        },
        "depth": {
            "write_ratio_pct": depth.get("write_ratio"),
            "collab_events": depth.get("collab_events"),
        },
        "exports": {
            "total_exports": exports.get("total_exports"),
            "exports_per_active_user": exports.get("exports_per_active_user"),
        },
        "kei": {
            "total_queries": kei.get("total_queries"),
            "users_with_queries": kei.get("users_with_queries"),
        },
        "jira": jira_sum or None,
        "salesforce": sf_sum or None,
        "cs_platform_health": cs_ph or None,
        "cs_supply_chain": cs_sc or None,
        "cs_platform_value": cs_pv or None,
        "people": {"champions_count": champions_n, "at_risk_users_count": at_risk_n},
        "feature_adoption_narrative": feat_narr,
    }


_SIGNALS_LLM_SYSTEM = """You are preparing Notable Signals for a customer QBR slide.

You receive JSON with:
- heuristic_signals: auto-generated lines (already merged from product analytics, support, CRM, CS exports). Treat every fact in them as authoritative.
- Structured fields (engagement, benchmarks, jira, salesforce, cs_*, people, feature narrative) for extra context. Do NOT invent numbers: only state metrics that appear in the input JSON.

Task:
- Produce at most {max_items} distinct, high-value signal lines for a CSM action list.
- Merge overlapping heuristic lines where it improves clarity; drop low-impact redundancy.
- Prioritize: commercial risk, support risk, adoption/engagement gaps, operational (supply/shortage) issues, then positives/wins.
- Each line is one concise sentence (no leading number like "1."; the slide template adds numbering).
- No markdown, no bullet characters at the start.

Return ONLY valid JSON:
{{"items":[{{"text":"<string>","theme":"<engagement|support|operations|commercial|product|people|other>"}}]}}

If heuristic_signals is empty, return {{"items":[]}}.
"""


_LEADING_ENUM_RE = re.compile(r"^\s*\d+[\.)]\s*")


def _normalize_item_text(text: str) -> str:
    t = " ".join((text or "").strip().split())
    t = _LEADING_ENUM_RE.sub("", t).strip()
    return t


def _parse_llm_signals(raw: str) -> list[str]:
    data = json.loads(_strip_json_code_fence(raw or ""))
    items = data.get("items")
    if not isinstance(items, list):
        return []
    out: list[str] = []
    for it in items:
        if isinstance(it, str):
            t = _normalize_item_text(it)
        elif isinstance(it, dict):
            t = _normalize_item_text(str(it.get("text") or ""))
        else:
            continue
        if len(t) < 8:
            continue
        out.append(t)
        if len(out) >= BPO_SIGNALS_LLM_MAX_ITEMS:
            break
    return out


def maybe_rewrite_signals_with_llm(report: dict[str, Any]) -> None:
    """If ``BPO_SIGNALS_LLM`` is on, replace ``report['signals']`` with LLM output; else no-op.

    On any failure, leaves ``report['signals']`` unchanged and sets ``report['_signals_llm_meta']``.
    """
    report.pop("_signals_llm_meta", None)
    if not BPO_SIGNALS_LLM:
        return

    base = list(report.get("signals") or [])
    if not base:
        report["_signals_llm_meta"] = {"source": "skipped", "reason": "no_heuristic_signals"}
        return

    try:
        client = llm_client()
    except RuntimeError as e:
        logger.warning("signals_llm: no LLM client (%s); keeping heuristic signals", e)
        report["_signals_llm_meta"] = {"source": "heuristic", "reason": "no_llm_client"}
        return

    payload = build_signals_llm_payload(report)
    user_json = json.dumps(payload, separators=(",", ":"), default=str)
    if len(user_json) > 14000:
        user_json = user_json[:13900] + "…"

    system = _SIGNALS_LLM_SYSTEM.format(max_items=BPO_SIGNALS_LLM_MAX_ITEMS)
    try:
        resp = _llm_create_with_retry(
            client,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=4096,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_json},
            ],
        )
        raw = resp.choices[0].message.content or ""
        lines = _parse_llm_signals(raw)
    except Exception as e:
        logger.warning("signals_llm: LLM call failed (%s); keeping heuristic signals", e)
        report["_signals_llm_meta"] = {"source": "heuristic", "reason": "llm_error", "detail": str(e)[:120]}
        return

    if not lines:
        logger.warning("signals_llm: empty or invalid JSON items; keeping heuristic signals")
        report["_signals_llm_meta"] = {"source": "heuristic", "reason": "empty_llm_output"}
        return

    report["signals"] = lines
    report["_signals_llm_meta"] = {"source": "llm", "count": len(lines)}
