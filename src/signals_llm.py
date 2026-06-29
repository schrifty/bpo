"""Optional LLM rewrite of Notable Signals — heuristic + cross-source facts (Phase 1 rules),
plus optional QBR Manifest + YAML slide brief as editorial context (Phase 3).

Enabled with ``CORTEX_SIGNALS_LLM=1``. Runs after ``extend_health_report_signals`` on the full health report.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from typing import Any

from .config import (
    CORTEX_SIGNALS_LLM,
    CORTEX_SIGNALS_LLM_EDITORIAL,
    CORTEX_SIGNALS_LLM_MAX_ITEMS,
    CORTEX_SIGNALS_LLM_MANIFEST_MAX_CHARS,
    CORTEX_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS,
    LLM_MODEL,
    llm_client,
    logger,
)
from .cs_report_client import get_csr_section
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence

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

    csr = get_csr_section(report)
    ph = csr.get("platform_health") or {}
    cs_ph: dict[str, Any] = {}
    if ph and not ph.get("error"):
        cs_ph = {
            "health_distribution": ph.get("health_distribution"),
            "total_critical_shortages": ph.get("total_critical_shortages"),
            "total_shortages": ph.get("total_shortages"),
            "factory_count": ph.get("factory_count"),
        }

    sc = csr.get("supply_chain") or {}
    cs_sc: dict[str, Any] = {}
    if sc and not sc.get("error"):
        cs_sc = {"totals": sc.get("totals"), "factory_count": sc.get("factory_count")}

    pv = csr.get("platform_value") or {}
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
        "csr": {
            "platform_health": cs_ph or None,
            "supply_chain": cs_sc or None,
            "platform_value": cs_pv or None,
        },
        "people": {"champions_count": champions_n, "at_risk_users_count": at_risk_n},
        "feature_adoption_narrative": feat_narr,
        "signals_trend_context": report.get("signals_trend_context"),
    }


def extract_executive_signals_slide_prompt(
    customer: str,
    *,
    max_chars: int | None = None,
) -> str | None:
    """Return the ``prompt`` text from the ``signals`` slide in ``executive_summary`` deck YAML (if any)."""
    from .deck_loader import resolve_deck

    cap = max_chars if max_chars is not None else CORTEX_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS
    try:
        r = resolve_deck("executive_summary", customer)
    except Exception as e:
        logger.debug("signals_llm: resolve_deck for slide prompt failed: %s", e)
        return None
    if not r or r.get("error"):
        return None
    for entry in r.get("slides") or []:
        st = entry.get("slide_type") or entry.get("id")
        if st != "signals":
            continue
        p = (entry.get("prompt") or "").strip()
        if not p:
            return None
        return p[:cap] if len(p) > cap else p
    return None


def build_signals_llm_user_envelope(
    report: dict[str, Any],
    *,
    manifest_rules: str | None,
    slide_prompt: str | None,
) -> dict[str, Any]:
    """Wrap ``facts`` plus optional ``editorial`` (Manifest + slide YAML) for the user message."""
    facts = build_signals_llm_payload(report)
    envelope: dict[str, Any] = {"facts": facts}
    if not CORTEX_SIGNALS_LLM_EDITORIAL:
        return envelope
    editorial: dict[str, str] = {}
    if manifest_rules and manifest_rules.strip():
        mr = manifest_rules.strip()
        mx = CORTEX_SIGNALS_LLM_MANIFEST_MAX_CHARS
        editorial["manifest_rules"] = mr[:mx] if len(mr) > mx else mr
    if slide_prompt and slide_prompt.strip():
        sp = slide_prompt.strip()
        cap = CORTEX_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS
        editorial["slide_brief_from_yaml"] = sp[:cap] if len(sp) > cap else sp
    if editorial:
        envelope["editorial"] = editorial
    return envelope


_SIGNALS_LLM_SYSTEM = """You are preparing Notable Signals for a customer QBR slide.

You receive JSON. The object has:
- "facts": same structure as before — heuristic_signals (authoritative strings) plus structured fields
  (engagement, benchmarks, jira, salesforce, cs_*, people, feature narrative). Do NOT invent numbers:
  only state metrics that appear in facts JSON.
- Optional "editorial": may contain "manifest_rules" (QBR Manifest from Google Docs) and/or
  "slide_brief_from_yaml" (instructions from the deck YAML for the Notable Signals slide).
  Use editorial only for audience, tone, emphasis, ordering, and what to de-emphasize or skip.
  Editorial cannot add new factual claims; if it asks for a metric not in facts, omit it.
- facts.signals_trend_context (when present): precomputed comparisons — WoW, MoM, prior_same_length
  (QoQ-style when the primary window is quarter-like), optional YoY. Each block includes
  weekly_active_rate_pct deltas and cohort medians under facts.benchmarks / cohort.

Task:
- Produce at most {max_items} distinct, high-value signal lines for a CSM / QBR action list.
- Where a trend comparison is relevant, cite **one** primary horizon in the signal text
  (e.g. "WoW", "MoM", "QoQ", "YoY") and use only numbers from signals_trend_context or engagement.
- Choose WoW for recent operational swings, MoM for monthly steering, QoQ / prior_same_length when
  the primary window matches quarterly reviews, YoY only if facts.signals_trend_context.yoy exists.
- Merge overlapping heuristic lines where it improves clarity; drop low-impact redundancy.
- Default priority when editorial is silent: commercial risk, support risk, adoption/engagement gaps,
  operational (supply/shortage) issues, then positives/wins.
- Each line is one concise sentence (no leading number like "1."; the slide template adds numbering).
- No markdown, no bullet characters at the start.

Also set:
- "trend_summary_for_slide": one line (max ~220 chars) for the slide subtitle area: lead with your
  chosen horizon (WoW/MoM/QoQ/YoY) and state the most important rate change plus cohort context
  (cohort median vs this account) when facts support it. Omit if signals_trend_context is missing or empty.
- "preferred_comparison_horizon": one of WoW | MoM | QoQ | YoY | mixed | prior_period

Return ONLY valid JSON:
{{"items":[{{"text":"<string>","theme":"<engagement|support|operations|commercial|product|people|other>"}}],
 "trend_summary_for_slide":"<string or empty>",
 "preferred_comparison_horizon":"<WoW|MoM|QoQ|YoY|mixed|prior_period>"}}

If facts.heuristic_signals is empty, return {{"items":[],"trend_summary_for_slide":"","preferred_comparison_horizon":""}}.
"""


_LEADING_ENUM_RE = re.compile(r"^\s*\d+[\.)]\s*")


def _normalize_item_text(text: str) -> str:
    t = " ".join((text or "").strip().split())
    t = _LEADING_ENUM_RE.sub("", t).strip()
    return t


_LLM_HORIZON_OK = frozenset({"WoW", "MoM", "QoQ", "YoY", "mixed", "prior_period"})


def _parse_llm_signals_response(raw: str) -> dict[str, Any]:
    data = json.loads(_strip_json_code_fence(raw or ""))
    items = data.get("items")
    if not isinstance(items, list):
        items = []
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
        if len(out) >= CORTEX_SIGNALS_LLM_MAX_ITEMS:
            break
    ts_raw = data.get("trend_summary_for_slide")
    trend_summary = str(ts_raw).strip()[:240] if ts_raw else ""
    if not trend_summary:
        trend_summary = ""
    ph_raw = data.get("preferred_comparison_horizon")
    horizon = str(ph_raw).strip() if ph_raw else ""
    if horizon not in _LLM_HORIZON_OK:
        horizon = ""
    return {
        "items": out,
        "trend_summary_for_slide": trend_summary,
        "preferred_comparison_horizon": horizon,
    }


def maybe_rewrite_signals_with_llm(report: dict[str, Any]) -> None:
    """If ``CORTEX_SIGNALS_LLM`` is on, replace ``report['signals']`` with LLM output; else no-op.

    Consumes and removes ``_signals_llm_manifest_rules`` / ``_signals_llm_slide_prompt`` if present
    (QBR passes Manifest + YAML brief). On failure, leaves heuristic ``signals`` and sets meta.
    """
    report.pop("_signals_llm_meta", None)
    manifest_rules = report.pop("_signals_llm_manifest_rules", None)
    slide_prompt = report.pop("_signals_llm_slide_prompt", None)

    if not CORTEX_SIGNALS_LLM:
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

    envelope = build_signals_llm_user_envelope(
        report,
        manifest_rules=manifest_rules if isinstance(manifest_rules, str) else None,
        slide_prompt=slide_prompt if isinstance(slide_prompt, str) else None,
    )
    user_json = json.dumps(envelope, separators=(",", ":"), default=str)
    if len(user_json) > 18000:
        user_json = user_json[:17900] + "…"

    system = _SIGNALS_LLM_SYSTEM.format(max_items=CORTEX_SIGNALS_LLM_MAX_ITEMS)
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
        parsed = _parse_llm_signals_response(raw)
        lines = parsed["items"]
    except Exception as e:
        logger.warning("signals_llm: LLM call failed (%s); keeping heuristic signals", e)
        report["_signals_llm_meta"] = {"source": "heuristic", "reason": "llm_error", "detail": str(e)[:120]}
        return

    if not lines:
        logger.warning("signals_llm: empty or invalid JSON items; keeping heuristic signals")
        report["_signals_llm_meta"] = {"source": "heuristic", "reason": "empty_llm_output"}
        return

    report["signals"] = lines
    if parsed.get("trend_summary_for_slide"):
        report["signals_trends_display"] = parsed["trend_summary_for_slide"]
    report["_signals_llm_meta"] = {
        "source": "llm",
        "count": len(lines),
        "editorial": bool(envelope.get("editorial")),
        "comparison_horizon": parsed.get("preferred_comparison_horizon") or None,
    }


# ── Portfolio Critical Signals (portfolio_review / csm_book_of_business) ───────────────

_PORTFOLIO_SIGNALS_LLM_USER_JSON_CAP = 26_000
_MAX_PORTFOLIO_CUSTOMER_FACT_ROWS = 95
_MAX_PER_CUSTOMER_SIGNAL_STRS = 10
_MAX_COHORT_DIGEST_BUCKETS = 18


def extract_portfolio_signals_slide_prompt(
    deck_id: str,
    *,
    max_chars: int | None = None,
) -> str | None:
    """YAML ``prompt`` for ``portfolio_signals`` from *deck_id* (portfolio or CSM book deck)."""
    from .deck_loader import resolve_deck

    did = deck_id if deck_id in ("portfolio_review", "csm_book_of_business") else "portfolio_review"
    cap = max_chars if max_chars is not None else CORTEX_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS
    try:
        r = resolve_deck(did, "Portfolio")
    except Exception as e:
        logger.debug("portfolio_signals_llm: resolve_deck for slide prompt failed: %s", e)
        return None
    if not r or r.get("error"):
        return None
    for entry in r.get("slides") or []:
        st = entry.get("slide_type") or entry.get("id")
        if st != "portfolio_signals":
            continue
        p = (entry.get("prompt") or "").strip()
        if not p:
            return None
        return p[:cap] if len(p) > cap else p
    return None


def _compact_portfolio_revenue_for_llm(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or raw.get("error"):
        return None
    top = raw.get("top_customers_by_arr") or []
    top_slim: list[dict[str, Any]] = []
    for row in top[:12]:
        if not isinstance(row, dict):
            continue
        top_slim.append(
            {
                "customer": _trim_str(row.get("customer") or row.get("name"), 80),
                "arr": row.get("arr") or row.get("ARR__c") or row.get("total_arr"),
            }
        )
    return {
        "total_arr": raw.get("total_arr"),
        "pipeline_arr": raw.get("pipeline_arr"),
        "active_installed_base_arr": raw.get("active_installed_base_arr"),
        "churned_contract_arr": raw.get("churned_contract_arr"),
        "active_customer_count": raw.get("active_customer_count"),
        "churned_customer_count": raw.get("churned_customer_count"),
        "salesforce_matched_customers": raw.get("salesforce_matched_customers"),
        "salesforce_unmatched_customers": raw.get("salesforce_unmatched_customers"),
        "top_customers_by_arr_sample": top_slim,
    }


def _compact_jira_help_metrics_for_llm(blob: Any) -> dict[str, Any] | None:
    if not isinstance(blob, dict) or blob.get("error"):
        return None
    bto = blob.get("by_type_open") or {}
    bso = blob.get("by_status_open") or {}
    if isinstance(bto, dict):
        bto = dict(list(bto.items())[:10])
    if isinstance(bso, dict):
        bso = dict(list(bso.items())[:10])
    ttfr = blob.get("ttfr_1y") if isinstance(blob.get("ttfr_1y"), dict) else {}
    ttr = blob.get("ttr_1y") if isinstance(blob.get("ttr_1y"), dict) else {}
    return {
        "unresolved_help": blob.get("unresolved_count"),
        "resolved_in_6mo_help": blob.get("resolved_in_6mo_count"),
        "by_type_open": bto,
        "by_status_open": bso,
        "ttfr_1y": {k: ttfr.get(k) for k in ("measured", "breached", "median", "avg") if k in ttfr},
        "ttr_1y": {k: ttr.get(k) for k in ("measured", "median", "avg") if k in ttr},
    }


def _portfolio_leaders_compact(leaders: Any, *, n: int = 3) -> dict[str, Any]:
    if not isinstance(leaders, dict):
        return {}
    out: dict[str, Any] = {}
    for key, rows in leaders.items():
        if not isinstance(rows, list):
            continue
        slim: list[dict[str, Any]] = []
        for row in rows[:n]:
            if isinstance(row, dict):
                slim.append(dict(row))
        out[str(key)] = slim
    return out


def _cohort_digest_compact(digest: Any) -> list[dict[str, Any]]:
    if not isinstance(digest, dict):
        return []
    out: list[dict[str, Any]] = []
    for cid, block in list(digest.items())[:_MAX_COHORT_DIGEST_BUCKETS]:
        if not isinstance(block, dict):
            continue
        out.append(
            {
                "cohort_id": str(cid),
                "display_name": _trim_str(block.get("display_name") or cid, 60),
                "n": block.get("n"),
                "median_login_pct": block.get("median_login_pct"),
                "median_write_ratio": block.get("median_write_ratio"),
                "kei_adoption_pct": block.get("kei_adoption_pct"),
                "customer_sample": [_trim_str(c, 48) for c in (block.get("customers") or [])[:6]],
            }
        )
    return out


def build_portfolio_signals_llm_payload(report: dict[str, Any]) -> dict[str, Any]:
    """Cross-customer facts for Critical Signals — Pendo summaries plus Jira / Salesforce rollups."""
    heur = list(report.get("portfolio_signals") or [])
    heur_slim = []
    for x in heur[:55]:
        if not isinstance(x, dict):
            continue
        heur_slim.append(
            {
                "customer": _trim_str(x.get("customer"), 80),
                "signal": _trim_str(x.get("signal"), 220),
                "severity": x.get("severity"),
            }
        )

    customers = [c for c in (report.get("customers") or []) if isinstance(c, dict)]
    fact_rows: list[dict[str, Any]] = []
    for s in customers[:_MAX_PORTFOLIO_CUSTOMER_FACT_ROWS]:
        eng = s.get("engagement") or {}
        fact_rows.append(
            {
                "customer": _trim_str(s.get("customer"), 80),
                "pendo_csm": _trim_str(s.get("pendo_csm"), 80),
                "login_pct": s.get("login_pct"),
                "active_users": s.get("active_users"),
                "total_users": s.get("total_users"),
                "benchmarks": {
                    "customer_active_rate": (s.get("benchmarks") or {}).get("customer_active_rate"),
                    "peer_median_rate": (s.get("benchmarks") or {}).get("peer_median_rate"),
                    "cohort_name": _trim_str((s.get("benchmarks") or {}).get("cohort_name"), 48),
                    "cohort_median_rate": (s.get("benchmarks") or {}).get("cohort_median_rate"),
                },
                "engagement": {
                    "active_7d": eng.get("active_7d"),
                    "active_30d": eng.get("active_30d"),
                    "dormant": eng.get("dormant"),
                    "active_rate_7d": eng.get("active_rate_7d"),
                },
                "depth": {
                    "write_ratio": (s.get("depth") or {}).get("write_ratio"),
                    "collab_events": (s.get("depth") or {}).get("collab_events"),
                },
                "kei": {
                    "total_queries": (s.get("kei") or {}).get("total_queries"),
                    "unique_users": (s.get("kei") or {}).get("unique_users"),
                    "adoption_rate": (s.get("kei") or {}).get("adoption_rate"),
                    "executive_users": (s.get("kei") or {}).get("executive_users"),
                },
                "guides": {
                    "dismiss_rate": (s.get("guides") or {}).get("dismiss_rate"),
                    "guide_reach": (s.get("guides") or {}).get("guide_reach"),
                },
                "exports": {
                    "total_exports": (s.get("exports") or {}).get("total_exports"),
                    "exports_per_active_user": (s.get("exports") or {}).get("exports_per_active_user"),
                },
                "per_customer_signal_lines": [
                    _trim_str(line, 200)
                    for line in (s.get("signals") or [])[:_MAX_PER_CUSTOMER_SIGNAL_STRS]
                    if line
                ],
            }
        )

    trends_block = report.get("portfolio_trends") or {}
    trends_list = trends_block.get("trends") if isinstance(trends_block, dict) else None
    trends_slim: list[dict[str, Any]] = []
    if isinstance(trends_list, list):
        for t in trends_list[:14]:
            if isinstance(t, dict):
                trends_slim.append(
                    {
                        "trend": _trim_str(t.get("trend"), 220),
                        "type": t.get("type"),
                        "customers": _trim_str(t.get("customers"), 120),
                    }
                )

    findings = report.get("cohort_findings_bullets") or []
    findings_slim = [_trim_str(b, 240) for b in findings[:16] if b]

    payload: dict[str, Any] = {
        "kind": "portfolio_critical_signals",
        "days": report.get("days"),
        "customer_count": report.get("customer_count"),
        "quarter": report.get("quarter"),
        "csm_owner": _trim_str(report.get("csm_owner"), 80) or None,
        "heuristic_critical_signals": heur_slim,
        "customers_fact_pack": fact_rows,
        "portfolio_trends": trends_slim,
        "portfolio_leaders": _portfolio_leaders_compact(report.get("portfolio_leaders")),
        "cohort_findings_bullets": findings_slim,
        "cohort_digest_compact": _cohort_digest_compact(report.get("cohort_digest")),
        "jira_help_portfolio_rollup": _compact_jira_help_metrics_for_llm(report.get("portfolio_help_ticket_metrics")),
        "salesforce_revenue_book_compact": _compact_portfolio_revenue_for_llm(report.get("portfolio_revenue_book")),
    }
    return payload


def build_portfolio_signals_llm_user_envelope(
    report: dict[str, Any],
    *,
    deck_id: str,
    slide_prompt: str | None,
) -> dict[str, Any]:
    facts = build_portfolio_signals_llm_payload(report)
    envelope: dict[str, Any] = {"facts": facts}
    if not CORTEX_SIGNALS_LLM_EDITORIAL:
        return envelope
    editorial: dict[str, str] = {}
    if slide_prompt and slide_prompt.strip():
        sp = slide_prompt.strip()
        cap = CORTEX_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS
        editorial["slide_brief_from_yaml"] = sp[:cap] if len(sp) > cap else sp
    if editorial:
        envelope["editorial"] = editorial
    return envelope


_PORTFOLIO_SIGNALS_LLM_SYSTEM = """You are selecting Critical Signals for a **portfolio-wide** executive deck
(cross-customer, not a single-account QBR).

You receive JSON with:
- "facts": a structured cross-customer pack. Key sections:
  - heuristic_critical_signals: ranked Pendo-derived alarm lines (authoritative themes; do not ignore).
  - customers_fact_pack: per-account engagement, depth, Kei, guides, exports, benchmarks, and each
    account's own heuristic signal lines (Pendo + behavioral).
  - portfolio_trends: pre-aggregated portfolio-level trend sentences.
  - portfolio_leaders: top customers by adoption / login / exports / write depth / exec Kei.
  - cohort_findings_bullets + cohort_digest_compact: cohort concentration and medians.
  - jira_help_portfolio_rollup: org-wide Jira HELP ticket backlog / type / status / SLA summaries (when present).
  - salesforce_revenue_book_compact: ARR, pipeline, churn vs active counts, top accounts by ARR (when present).
- Optional "editorial": "slide_brief_from_yaml" — audience and emphasis from the deck YAML only.

Rules:
- Output at most {max_items} signal rows. Each row names the customer when the signal is account-specific;
  use customer "Portfolio" only for org-wide / non-account themes.
- Prefer facts that combine **product telemetry (Pendo)** with **commercial (Salesforce)** or **operations /
  support (Jira)** when the JSON supports it. Do not invent metrics: every number or named entity must appear
  in facts (or be a short paraphrase of a heuristic line).
- Merge duplicate themes across customers where it clarifies a systemic issue; keep the highest-impact
  distinct rows when space is tight.
- Each "signal" is one concise sentence (no leading "1."; the slide adds numbering). No markdown.

Return ONLY valid JSON:
{{"items":[{{"customer":"<string>","signal":"<string>"}}]}}
"""


def _parse_portfolio_llm_signals_response(raw: str) -> list[dict[str, Any]]:
    data = json.loads(_strip_json_code_fence(raw or ""))
    items = data.get("items")
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for it in items:
        cust = ""
        sig = ""
        if isinstance(it, dict):
            cust = str(it.get("customer") or "").strip()
            sig = _normalize_item_text(str(it.get("signal") or ""))
        elif isinstance(it, str):
            sig = _normalize_item_text(it)
            if ":" in sig:
                left, right = sig.split(":", 1)
                if len(left) < 60:
                    cust, sig = left.strip(), right.strip()
        if len(sig) < 8:
            continue
        if not cust:
            cust = "Portfolio"
        out.append({"customer": cust, "signal": sig})
        if len(out) >= CORTEX_SIGNALS_LLM_MAX_ITEMS:
            break
    return out


def maybe_rewrite_portfolio_signals_with_llm(
    report: dict[str, Any],
    *,
    deck_id: str = "portfolio_review",
) -> None:
    """When ``CORTEX_SIGNALS_LLM`` is on, replace ``portfolio_signals`` using a rich multi-source envelope."""
    report.pop("_portfolio_signals_llm_meta", None)
    if not CORTEX_SIGNALS_LLM:
        return
    if not (report.get("customers") or []):
        return

    if report.get("portfolio_help_ticket_metrics") is None:
        try:
            from .jira_client import get_shared_jira_client

            report["portfolio_help_ticket_metrics"] = get_shared_jira_client().get_customer_ticket_metrics(None)
        except Exception as e:
            logger.warning("portfolio_signals_llm: HELP metrics fetch failed: %s", e)
            report["portfolio_help_ticket_metrics"] = {"error": str(e)[:200]}

    slide_prompt = extract_portfolio_signals_slide_prompt(deck_id)

    try:
        client = llm_client()
    except RuntimeError as e:
        logger.warning("portfolio_signals_llm: no LLM client (%s); keeping heuristic portfolio_signals", e)
        report["_portfolio_signals_llm_meta"] = {"source": "heuristic", "reason": "no_llm_client"}
        return

    envelope = build_portfolio_signals_llm_user_envelope(
        report,
        deck_id=deck_id,
        slide_prompt=slide_prompt,
    )
    user_json = json.dumps(envelope, separators=(",", ":"), default=str)
    if len(user_json) > _PORTFOLIO_SIGNALS_LLM_USER_JSON_CAP:
        user_json = user_json[: _PORTFOLIO_SIGNALS_LLM_USER_JSON_CAP - 3] + "…"

    system = _PORTFOLIO_SIGNALS_LLM_SYSTEM.format(max_items=CORTEX_SIGNALS_LLM_MAX_ITEMS)
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
        rows = _parse_portfolio_llm_signals_response(raw)
    except Exception as e:
        logger.warning("portfolio_signals_llm: LLM call failed (%s); keeping heuristic portfolio_signals", e)
        report["_portfolio_signals_llm_meta"] = {"source": "heuristic", "reason": "llm_error", "detail": str(e)[:120]}
        return

    if not rows:
        logger.warning("portfolio_signals_llm: empty LLM items; keeping heuristic portfolio_signals")
        report["_portfolio_signals_llm_meta"] = {"source": "heuristic", "reason": "empty_llm_output"}
        return

    report["portfolio_signals"] = rows
    report["_portfolio_signals_llm_meta"] = {
        "source": "llm",
        "count": len(rows),
        "editorial": bool(envelope.get("editorial")),
        "deck_id": deck_id,
    }
