"""Composite account churn-risk score for LLM export §7 (deterministic, no extra LLM calls)."""

from __future__ import annotations

from typing import Any

from .salesforce_client import _CHURNED_CONTRACT_STATUS_LOWER
from .salesforce_commercial_status import (
    COMMERCIAL_STATUS_ACTIVE,
    COMMERCIAL_STATUS_CHURNED,
    COMMERCIAL_STATUS_FUTURE,
    COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING,
)

# Align with pendo_client._compute_portfolio_signals alarm keywords.
_SIGNAL_KEYWORDS: tuple[str, ...] = (
    "no active users",
    "declining",
    "dropped",
    "no kei",
    "dismiss",
    "read-heavy",
    "low guide reach",
    "at risk",
    "churned",
)

_PILLAR_WEIGHTS: dict[str, float] = {
    "pendo": 0.35,
    "salesforce": 0.30,
    "signals": 0.20,
    "jira": 0.10,
    "cs_report": 0.05,
}

_TIER_CRITICAL = 75
_TIER_HIGH = 50
_TIER_MEDIUM = 25


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _safe_float(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _risk_tier(score: int) -> str:
    if score >= _TIER_CRITICAL:
        return "critical"
    if score >= _TIER_HIGH:
        return "high"
    if score >= _TIER_MEDIUM:
        return "medium"
    return "low"


def signal_severity(signal_text: str) -> int:
    """Count alarm-keyword hits in one portfolio signal line (same idea as Pendo portfolio signals)."""
    sig_lower = (signal_text or "").lower()
    return sum(1 for kw in _SIGNAL_KEYWORDS if kw in sig_lower)


def portfolio_signals_for_customer(
    portfolio_signals: list[Any],
    customer: str,
) -> list[dict[str, Any]]:
    """Structured portfolio signal rows for one customer."""
    c_low = (customer or "").strip().lower()
    if not c_low:
        return []
    out: list[dict[str, Any]] = []
    for item in portfolio_signals:
        if not isinstance(item, dict):
            continue
        cust = str(item.get("customer") or "").strip().lower()
        if cust and cust != c_low and c_low not in cust and cust not in c_low:
            continue
        sig = str(item.get("signal") or "").strip()
        if not sig:
            continue
        sev = item.get("severity")
        if sev is None:
            sev = signal_severity(sig)
        try:
            sev_i = max(0, int(sev))
        except (TypeError, ValueError):
            sev_i = signal_severity(sig)
        out.append({"signal": sig, "severity": sev_i})
    return out


def _pendo_subscores(pendo: dict[str, Any]) -> tuple[float, list[tuple[str, float, float]]]:
    """Return (pillar 0–100, leaf metrics with internal weight)."""
    leaves: list[tuple[str, float, float]] = []

    login = _safe_float(pendo.get("login_pct"))
    if login is not None:
        s = _clamp(100.0 - login, 0.0, 100.0)
        leaves.append(("Pendo login rate", s, 0.45))
    else:
        leaves.append(("Pendo login rate", 50.0, 0.45))

    kei = pendo.get("kei") if isinstance(pendo.get("kei"), dict) else {}
    tq = _safe_float(kei.get("total_queries"), 0.0) or 0.0
    adoption = _safe_float(kei.get("adoption_rate"))
    if tq <= 0:
        leaves.append(("Pendo KEI usage", 85.0, 0.30))
    elif adoption is not None:
        leaves.append(("Pendo KEI adoption", _clamp(100.0 - adoption, 0.0, 100.0), 0.30))
    else:
        leaves.append(("Pendo KEI adoption", 50.0, 0.30))

    guides = pendo.get("guides") if isinstance(pendo.get("guides"), dict) else {}
    dismiss = _safe_float(guides.get("dismiss_rate"))
    if dismiss is not None:
        leaves.append(("Pendo guide dismiss rate", _clamp(dismiss * 1.25, 0.0, 100.0), 0.15))
    else:
        leaves.append(("Pendo guide dismiss rate", 50.0, 0.15))

    eng = pendo.get("engagement") if isinstance(pendo.get("engagement"), dict) else {}
    active7 = _safe_float(eng.get("active_rate_7d"))
    if active7 is not None:
        leaves.append(("Pendo 7d active rate", _clamp(100.0 - active7, 0.0, 100.0), 0.10))

    w_sum = sum(w for _, _, w in leaves)
    pillar = sum(s * w for _, s, w in leaves) / w_sum if w_sum else 50.0
    return pillar, leaves


def _renewal_negotiation_subscore(
    sf: dict[str, Any],
    *,
    label_prefix: str = "Salesforce renewal in flight",
) -> tuple[float, list[tuple[str, float, float]]]:
    pipe = sf.get("pipeline_arr_including_parent_accounts")
    detail = (
        f"open renewal pipeline on parent account (${pipe:,.0f} ARR)"
        if isinstance(pipe, (int, float)) and pipe
        else "open renewal pipeline on parent account"
    )
    leaves = [(f"{label_prefix} ({detail})", 25.0, 1.0)]
    return 25.0, leaves


def _sf_contract_subscore(sf: dict[str, Any]) -> tuple[float, list[tuple[str, float, float]]]:
    leaves: list[tuple[str, float, float]] = []

    status = str(sf.get("commercial_status") or "").strip().upper() or None
    if status == COMMERCIAL_STATUS_CHURNED:
        leaves.append(("Salesforce CHURNED commercial_status", 100.0, 1.0))
        return 100.0, leaves
    if status == COMMERCIAL_STATUS_OUT_OF_CONTRACT_RENEWING:
        return _renewal_negotiation_subscore(
            sf,
            label_prefix="Salesforce OUT_OF_CONTRACT_RENEWING",
        )
    if status == COMMERCIAL_STATUS_FUTURE:
        leaves.append(("Salesforce FUTURE contract (not yet started)", 20.0, 1.0))
        return 20.0, leaves

    if status != COMMERCIAL_STATUS_ACTIVE:
        if sf.get("renewal_in_flight") is True:
            return _renewal_negotiation_subscore(sf)

        active = sf.get("active_in_salesforce")
        if active is False or sf.get("active") is False:
            leaves.append(("Salesforce contract status", 100.0, 1.0))
            return 100.0, leaves

    statuses = sf.get("contract_statuses_distinct")
    if isinstance(statuses, list):
        for st in statuses:
            if str(st).strip().lower() in _CHURNED_CONTRACT_STATUS_LOWER:
                leaves.append(("Salesforce churned contract", 100.0, 1.0))
                return 100.0, leaves

    days = _safe_float(sf.get("days_until_contract_end_nearest"))
    if days is None:
        leaves.append(("Salesforce renewal timing", 50.0, 1.0))
        return 50.0, leaves

    if days <= 0:
        s = 100.0
        detail = "contract ended or overdue"
    elif days <= 90:
        s = 100.0 - (days / 90.0) * 60.0
        detail = f"{int(days)} days to nearest contract end"
    elif days <= 365:
        s = 40.0 - ((days - 90.0) / 275.0) * 30.0
        detail = f"{int(days)} days to nearest contract end"
    else:
        s = 10.0
        detail = f"{int(days)} days to nearest contract end"

    leaves.append((f"Salesforce renewal timing ({detail})", _clamp(s, 0.0, 100.0), 1.0))
    return leaves[0][1], leaves


def _signals_subscore(rows: list[dict[str, Any]]) -> tuple[float, list[tuple[str, float, float]]]:
    if not rows:
        return 50.0, [("Portfolio signals", 50.0, 1.0)]
    severities = [max(0, int(r.get("severity") or 0)) for r in rows]
    severities = [s for s in severities if s > 0]
    if not severities:
        return 50.0, [("Portfolio signals", 50.0, 1.0)]
    n = len(severities)
    sum_sev = sum(severities)
    mx = max(severities)
    s = _clamp(15.0 * n + 12.0 * sum_sev + 8.0 * mx, 0.0, 100.0)
    label = f"Portfolio alarm signals ({n} line(s), peak severity {mx})"
    return s, [(label, s, 1.0)]


def _jira_subscores(jira: dict[str, Any]) -> tuple[float | None, list[tuple[str, float, float]]]:
    if not jira or jira.get("error"):
        return None, []
    leaves: list[tuple[str, float, float]] = []

    open_i = _safe_float(jira.get("open_issues"), 0.0) or 0.0
    leaves.append(("Jira HELP open issues", _clamp(open_i * 4.0, 0.0, 100.0), 0.35))

    esc = _safe_float(jira.get("escalated"), 0.0) or 0.0
    leaves.append(("Jira HELP escalations", _clamp(esc * 20.0, 0.0, 100.0), 0.30))

    tick = jira.get("customer_ticket_metrics")
    sla_s = 50.0
    if isinstance(tick, dict):
        sla = tick.get("sla_adherence_1y")
        if isinstance(sla, dict):
            pct = _safe_float(sla.get("pct"))
            if pct is not None:
                sla_s = _clamp(100.0 - pct, 0.0, 100.0)
                leaves.append((f"Jira HELP SLA adherence ({pct:.0f}% met)", sla_s, 0.20))
            else:
                leaves.append(("Jira HELP SLA adherence", 50.0, 0.20))
        unres = _safe_float(tick.get("unresolved_count"), 0.0) or 0.0
        leaves.append(("Jira HELP unresolved backlog", _clamp(unres * 3.0, 0.0, 100.0), 0.15))
    else:
        leaves.append(("Jira HELP SLA adherence", 50.0, 0.20))
        leaves.append(("Jira HELP unresolved backlog", 50.0, 0.15))

    w_sum = sum(w for _, _, w in leaves)
    pillar = sum(s * w for _, s, w in leaves) / w_sum if w_sum else 50.0
    return pillar, leaves


def _csr_subscore(csr_sites: list[dict[str, Any]]) -> tuple[float | None, list[tuple[str, float, float]]]:
    if not csr_sites:
        return None, []
    red = yellow = shortages = 0
    for s in csr_sites:
        if not isinstance(s, dict):
            continue
        hs = str(s.get("health_score") or "").strip().upper()
        if hs == "RED":
            red += 1
        elif hs == "YELLOW":
            yellow += 1
        try:
            shortages += int(s.get("shortages") or s.get("criticalShortagesCt") or 0)
        except (TypeError, ValueError):
            pass
    n = max(len(csr_sites), 1)
    c_health = _clamp((red * 100.0 + yellow * 50.0) / n, 0.0, 100.0)
    c_short = _clamp(shortages / 20.0 * 100.0, 0.0, 100.0)
    leaves = [
        (f"CS Report site health ({red} RED / {yellow} YELLOW)", c_health, 0.70),
        (f"CS Report shortages ({shortages} total)", c_short, 0.30),
    ]
    pillar = 0.70 * c_health + 0.30 * c_short
    return pillar, leaves


def _top_influencer(contributions: list[tuple[str, float]]) -> str:
    if not contributions:
        return "Insufficient data"
    label, _ = max(contributions, key=lambda x: x[1])
    return label


def compute_customer_risk_score(
    *,
    pendo: dict[str, Any] | None = None,
    salesforce: dict[str, Any] | None = None,
    portfolio_signals: list[dict[str, Any]] | None = None,
    csr_sites: list[dict[str, Any]] | None = None,
    jira_help: dict[str, Any] | None = None,
    include_jira: bool = True,
) -> dict[str, Any]:
    """Composite 0–100 risk score (higher = worse) with tier and top weighted driver label."""
    pendo = pendo if isinstance(pendo, dict) else {}
    salesforce = salesforce if isinstance(salesforce, dict) else {}
    portfolio_signals = portfolio_signals if isinstance(portfolio_signals, list) else []
    csr_sites = csr_sites if isinstance(csr_sites, list) else []

    pillars: dict[str, float] = {}
    all_contribs: list[tuple[str, float]] = []

    p_pendo, pendo_leaves = _pendo_subscores(pendo)
    pillars["pendo"] = p_pendo
    w_p = _PILLAR_WEIGHTS["pendo"]
    for label, sub, lw in pendo_leaves:
        all_contribs.append((label, w_p * lw * sub))

    p_sf, sf_leaves = _sf_contract_subscore(salesforce)
    pillars["salesforce"] = p_sf
    w_s = _PILLAR_WEIGHTS["salesforce"]
    for label, sub, lw in sf_leaves:
        all_contribs.append((label, w_s * lw * sub))

    p_sig, sig_leaves = _signals_subscore(portfolio_signals)
    pillars["signals"] = p_sig
    w_g = _PILLAR_WEIGHTS["signals"]
    for label, sub, lw in sig_leaves:
        all_contribs.append((label, w_g * lw * sub))

    weight_used = w_p + w_s + w_g
    score_acc = w_p * p_pendo + w_s * p_sf + w_g * p_sig

    p_csr, csr_leaves = _csr_subscore(csr_sites)
    if p_csr is not None:
        pillars["cs_report"] = p_csr
        w_c = _PILLAR_WEIGHTS["cs_report"]
        weight_used += w_c
        score_acc += w_c * p_csr
        for label, sub, lw in csr_leaves:
            all_contribs.append((label, w_c * lw * sub))

    if include_jira:
        p_jira, jira_leaves = _jira_subscores(jira_help if isinstance(jira_help, dict) else {})
        if p_jira is not None:
            pillars["jira"] = p_jira
            w_j = _PILLAR_WEIGHTS["jira"]
            weight_used += w_j
            score_acc += w_j * p_jira
            for label, sub, lw in jira_leaves:
                all_contribs.append((label, w_j * lw * sub))

    risk_score = int(round(score_acc / weight_used)) if weight_used else 50
    risk_score = int(_clamp(float(risk_score), 0.0, 100.0))

    top = _top_influencer(all_contribs)
    # Enrich login influencer with actual %
    login_pct = _safe_float(pendo.get("login_pct"))
    if top.startswith("Pendo login") and login_pct is not None:
        top = f"Pendo login rate ({login_pct:.0f}% active users)"

    return {
        "risk_score": risk_score,
        "risk_tier": _risk_tier(risk_score),
        "top_influencer": top,
        "pillars": {k: round(v, 1) for k, v in pillars.items()},
    }

