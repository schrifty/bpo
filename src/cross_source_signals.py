"""Append Notable Signals lines from Jira, Salesforce, CS Report, Pendo people & features.

Uses data already present on ``get_customer_health_report`` — no extra API calls.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from .cs_report_client import get_csr_section


# Cap total signals after enrichment (slide paginates; avoid unbounded lists).
_MAX_TOTAL_SIGNALS = 22


def _norm_snippet(s: str, max_len: int = 48) -> str:
    t = " ".join(s.lower().split())
    return t[:max_len]


def _redundant_with_existing(existing: list[str], candidate: str) -> bool:
    """Skip if candidate is substantially duplicated with an existing line."""
    c = _norm_snippet(candidate, 64)
    if len(c) < 12:
        return False
    for e in existing:
        e2 = _norm_snippet(e, 64)
        if c in e2 or e2 in c:
            return True
    return False


def _days_until_contract_end(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    s = str(raw).strip()[:10]
    try:
        end = _dt.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None
    return (end - _dt.date.today()).days


def _fmt_money_compact(n: float) -> str:
    a = abs(n)
    if a >= 1_000_000:
        return f"${n / 1_000_000:,.1f}M"
    if a >= 1_000:
        return f"${n / 1_000:,.0f}K"
    return f"${n:,.0f}"


def _collect_jira_signals(jira: dict[str, Any]) -> list[str]:
    if not jira or jira.get("error"):
        return []
    out: list[str] = []
    days = int(jira.get("days") or 90)
    esc = int(jira.get("escalated") or 0)
    if esc >= 1:
        out.append(
            f"Support: {esc} escalated or engineering-queue ticket(s) (Jira HELP, {days}d)"
        )

    ob = int(jira.get("open_bugs") or 0)
    if ob >= 2:
        out.append(f"Support: {ob} open bug(s) in Jira HELP")

    open_i = int(jira.get("open_issues") or 0)
    res_i = int(jira.get("resolved_issues") or 0)
    total = int(jira.get("total_issues") or 0)
    if open_i >= 10:
        out.append(
            f"Support: {open_i} open HELP tickets ({res_i} resolved, {total} in {days}d window)"
        )
    elif open_i >= 6 and open_i > max(res_i, 1):
        out.append(
            f"Support: open backlog ({open_i} open vs {res_i} resolved in Jira HELP, {days}d)"
        )

    for label, block in (("TTFR", jira.get("ttfr")), ("TTR", jira.get("ttr"))):
        if not isinstance(block, dict):
            continue
        measured = int(block.get("measured") or 0)
        breached = int(block.get("breached") or 0)
        if measured >= 5 and breached >= 3:
            out.append(
                f"Support: {label} SLA breached on {breached} of {measured} measured HELP tickets"
            )
    return out


def _collect_cs_platform_signals(ph: dict[str, Any]) -> list[str]:
    if not ph or ph.get("error"):
        return []
    out: list[str] = []
    dist = ph.get("health_distribution") or {}
    reds = int(dist.get("RED", 0) or 0)
    yellows = int(dist.get("YELLOW", 0) or 0)
    greens = int(dist.get("GREEN", 0) or 0)
    if reds >= 1:
        out.append(f"Operations (CS Report): {reds} factory site(s) at RED health score")
    elif yellows >= 2 and greens == 0 and (reds + yellows + greens) > 0:
        out.append(
            f"Operations (CS Report): no GREEN sites ({yellows} YELLOW, {reds} RED) — review factory health"
        )

    tc = int(ph.get("total_critical_shortages") or 0)
    if tc >= 3:
        out.append(
            f"Operations (CS Report): {tc} critical shortage items flagged across factories"
        )
    ts = int(ph.get("total_shortages") or 0)
    if ts >= 40 and tc < 3:
        out.append(
            f"Operations (CS Report): {ts} shortage items across sites — inventory risk"
        )
    return out


def _collect_cs_supply_signals(sc: dict[str, Any]) -> list[str]:
    if not sc or sc.get("error"):
        return []
    out: list[str] = []
    totals = sc.get("totals") or {}
    past_po = float(totals.get("past_due_po", 0) or 0)
    past_req = float(totals.get("past_due_req", 0) or 0)
    past = past_po + past_req
    if past >= 250_000:
        out.append(
            f"Supply chain (CS Report): {_fmt_money_compact(past)} past-due PO + requirement value"
        )

    late = 0
    for s in sc.get("sites") or []:
        late += int(s.get("late_pos", 0) or 0) + int(s.get("late_prs", 0) or 0)
    if late >= 20:
        out.append(f"Supply chain (CS Report): {late} late PO/PR counts in the export window")
    return out


def _collect_cs_value_signals(pv: dict[str, Any]) -> list[str]:
    if not pv or pv.get("error"):
        return []
    out: list[str] = []
    overdue = int(pv.get("total_overdue_tasks") or 0)
    if overdue >= 8:
        out.append(
            f"Value delivery (CS Report): {overdue} overdue workbench tasks across sites"
        )
    recs = int(pv.get("total_recs_created_30d") or 0)
    savings = float(pv.get("total_savings") or 0)
    open_ia = float(pv.get("total_open_ia_value") or 0)
    factories = int(pv.get("factory_count") or 0)
    if factories >= 1 and recs == 0 and savings == 0 and open_ia < 1_000:
        out.append(
            "Value delivery (CS Report): no recs created and no period savings in export — IA momentum?"
        )
    return out


def _collect_salesforce_signals(sf: dict[str, Any]) -> list[str]:
    if not sf or not sf.get("matched"):
        return []
    out: list[str] = []
    accounts = sf.get("accounts") or []
    total_arr = 0.0
    soonest: tuple[int, str] | None = None
    for a in accounts:
        try:
            v = float(a.get("ARR__c") or 0)
        except (TypeError, ValueError):
            v = 0.0
        total_arr += v
        name = (a.get("Name") or a.get("LeanDNA_Entity_Name__c") or "Account").strip()
        du = _days_until_contract_end(a.get("Contract_Contract_End_Date__c"))
        if du is not None and 0 < du <= 120:
            if soonest is None or du < soonest[0]:
                soonest = (du, name)

    pipeline = float(sf.get("pipeline_arr") or 0)
    if pipeline >= 50_000:
        out.append(
            f"Commercial (Salesforce): {_fmt_money_compact(pipeline)} pipeline ARR in active stages"
        )

    opps = int(sf.get("opportunity_count_this_year") or 0)
    if total_arr >= 25_000 and opps == 0:
        out.append(
            "Commercial (Salesforce): no opportunities created this year while ARR is on file — check CRM hygiene"
        )

    if soonest is not None:
        d, nm = soonest
        out.append(
            f"Commercial (Salesforce): contract end in ~{d} days ({nm}) — renewal timing"
        )
    return out


def _collect_people_signals(report: dict[str, Any]) -> list[str]:
    champions = report.get("champions") or []
    at_risk = report.get("at_risk_users") or []
    if not isinstance(champions, list):
        champions = []
    if not isinstance(at_risk, list):
        at_risk = []

    acct = report.get("account") or {}
    total_visitors = int(acct.get("total_visitors") or 0)

    out: list[str] = []
    if total_visitors >= 12 and len(champions) == 0:
        out.append(
            "People: no named champions in Pendo — identify executive or power-user sponsors"
        )
    if len(at_risk) >= 4:
        out.append(
            f"People: {len(at_risk)} users flagged at-risk for dormancy — re-engagement outreach"
        )
    return out


def _collect_feature_signals(report: dict[str, Any]) -> list[str]:
    insights = report.get("feature_adoption_insights")
    if not isinstance(insights, dict):
        return []
    n = insights.get("narrative")
    if not n or not isinstance(n, str):
        return []
    line = " ".join(n.strip().split())
    if len(line) > 160:
        line = line[:157] + "…"
    return [f"Product usage: {line}"]


def _ordered_cross_source_candidates(report: dict[str, Any]) -> list[str]:
    """Higher-priority signals first (support risk → ops → commercial → people → usage narrative)."""
    csr = get_csr_section(report)
    chunks: list[list[str]] = [
        _collect_jira_signals(report.get("jira") or {}),
        _collect_cs_platform_signals(csr.get("platform_health") or {}),
        _collect_cs_supply_signals(csr.get("supply_chain") or {}),
        _collect_cs_value_signals(csr.get("platform_value") or {}),
        _collect_salesforce_signals(report.get("salesforce") or {}),
        _collect_people_signals(report),
        _collect_feature_signals(report),
    ]
    out: list[str] = []
    for part in chunks:
        out.extend(part)
    return out


def extend_health_report_signals(report: dict[str, Any]) -> None:
    """Mutate ``report['signals']`` with cross-source lines; cap total length in-place."""
    base = list(report.get("signals") or [])
    candidates = _ordered_cross_source_candidates(report)
    for cand in candidates:
        if len(base) >= _MAX_TOTAL_SIGNALS:
            break
        if _redundant_with_existing(base, cand):
            continue
        base.append(cand)
    report["signals"] = base
