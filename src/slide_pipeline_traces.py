"""Canonical speaker-note pipeline traces for slide builders."""

from __future__ import annotations

from typing import Any

from .cs_report_client import get_csr_section
from .slide_loader import (
    benchmarks_min_peers_for_cohort_median,
    cohort_findings_min_customers_for_cross_cohort_compare,
)
from .slides_theme import (
    _CohortProfileTraceLabels,
    _CohortSummaryLabels,
    _HealthSnapshotLabels,
    _cohort_summary_metrics,
    _truncate_kpi_card_label,
)
from .speaker_notes import build_slide_jql_speaker_notes


def fmt_platform_value_dollar(value: float) -> str:
    absolute = abs(float(value))
    if absolute >= 1_000_000_000:
        return f"${value / 1_000_000_000:,.2f}B"
    if absolute >= 1_000_000:
        return f"${value / 1_000_000:,.1f}M"
    if absolute >= 1_000:
        return f"${value / 1_000:,.0f}K"
    return f"${value:,.0f}"


def fmt_platform_value_count(value: int | float) -> str:
    number = int(value)
    absolute = abs(number)
    if absolute >= 1_000_000:
        return f"{number / 1_000_000:,.1f}M"
    if absolute >= 100_000:
        return f"{number / 1_000:,.0f}K"
    return f"{number:,}"


def health_snapshot_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Account Health Snapshot."""
    eng = report.get("engagement") or {}
    bench = report.get("benchmarks") or {}
    acct = report.get("account") or {}
    if not eng or not bench or not acct:
        return []
    labels = _HealthSnapshotLabels
    rate = eng.get("active_rate_7d")
    cohort_name = (bench.get("cohort_name") or "").strip()
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count") or 0
    min_peers = benchmarks_min_peers_for_cohort_median()
    use_cohort = cohort_med is not None and cohort_n >= min_peers
    if use_cohort:
        vs = rate - cohort_med
        bench_label = f"{cohort_name} median of {cohort_med}%  ({cohort_n} peers)"
    else:
        vs = rate - bench.get("peer_median_rate", 0)
        bench_label = f"all-customer median of {bench.get('peer_median_rate')}%  ({bench.get('peer_count')} peers)"
    direction = "above" if vs > 0 else "below" if vs < 0 else "at"

    rows: list[dict[str, str]] = [
        {
            "description": labels.CUSTOMER_USERS,
            "source": "Pendo",
            "query": "account.total_visitors — visitors attributed to this customer (metadata / sitenames rollup)",
        },
        {
            "description": labels.ACTIVE_THIS_WEEK,
            "source": "Pendo",
            "query": "engagement.active_7d; on-slide % is active_rate_7d (= active_7d / total_visitors)",
        },
        {
            "description": labels.ACTIVE_THIS_MONTH,
            "source": "Pendo",
            "query": "Sum of active_7d + active_30d engagement buckets (counts on slide)",
        },
        {
            "description": labels.DORMANT,
            "source": "Pendo",
            "query": "engagement.dormant — no activity in 30+ days",
        },
        {
            "description": labels.WEEKLY_ACTIVE_RATE,
            "source": "Pendo",
            "query": (
                f"Same % as row above; {abs(vs):.0f}pp {direction} {bench_label} "
                f"(cohort from cohorts.yaml when n≥{min_peers}; slides/std-07-benchmarks.yaml rollup_params)"
            ),
        },
        {
            "description": labels.SITES,
            "source": "Pendo",
            "query": "account.total_sites from visitor sitenames linked to this customer",
        },
        {
            "description": labels.COHORT,
            "source": "Pendo + cohorts.yaml",
            "query": "Label from get_customer_cohort / cohorts.yaml (shows Unclassified when missing)",
        },
    ]
    internal = int(acct.get("internal_visitors") or 0)
    if internal:
        rows.append({
            "description": "Internal staff excluded",
            "source": "Pendo",
            "query": "LeanDNA/internal visitors removed from customer engagement totals",
        })
    return rows


def peer_benchmarks_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Peer Benchmarks."""
    bench = report.get("benchmarks") or {}
    if not bench:
        return []
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    cohort_name = bench.get("cohort_name", "")
    min_peers = benchmarks_min_peers_for_cohort_median()
    use_cohort = cohort_med is not None and cohort_n >= min_peers

    q_rate = (
        "active_7d / total_visitors over the report window; "
        "7-day activity from visitor time-bucket aggregation"
    )
    q_all_median = (
        "Median of weekly active rate across accounts with Pendo data in the same period "
        "(peer_count on payload)"
    )
    q_cohort_median = (
        "Median among accounts in the same manufacturing cohort "
        f"(get_customer_cohort / cohorts.yaml); shown when cohort n≥{min_peers} "
        "(rollup_params on slides/std-07-benchmarks.yaml)"
    )
    q_delta = "Customer weekly active rate minus comparison median (percentage points vs peer/cohort on slide)"
    q_acct = "account.total_visitors and account.total_sites for the account size line under KPI row"

    out = [{"description": "Weekly active rate (this account)", "source": "Pendo", "query": q_rate}]
    if use_cohort:
        med_lbl = _truncate_kpi_card_label(f"{cohort_name} median ({cohort_n} accounts)")
        out.append({"description": med_lbl, "source": "Pendo", "query": q_cohort_median})
        all_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
        out.append({"description": all_lbl, "source": "Pendo", "query": q_all_median})
    else:
        med_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
        out.append({"description": med_lbl, "source": "Pendo", "query": q_all_median})

    out.append({"description": "Delta", "source": "Pendo", "query": q_delta})
    out.append({"description": "Account size", "source": "Pendo", "query": q_acct})
    return out


def platform_value_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Platform Value & ROI."""
    cs = get_csr_section(report).get("platform_value")
    if not isinstance(cs, dict) or cs.get("error"):
        return []
    total_savings = float(cs.get("total_savings") or 0)
    total_open = float(cs.get("total_open_ia_value") or 0)
    total_recs = int(cs.get("total_recs_created_30d") or 0)
    total_pos = int(cs.get("total_pos_placed_30d") or 0)
    total_overdue = int(cs.get("total_overdue_tasks") or 0)
    factory_count = int(cs.get("factory_count") or 0)
    site_list = cs.get("sites") or []
    factory_rows = [site for site in site_list if site.get("savings_current_period") or site.get("recs_created_30d")]
    n_table = len(factory_rows)

    return [
        {
            "description": "Savings achieved",
            "source": "CS Report",
            "query": (
                f"On-slide value {fmt_platform_value_dollar(total_savings)} — sum of "
                "inventoryActionCurrentReportingPeriodSavings endValue across customer week rows "
                f"({factory_count} factories)"
            ),
        },
        {
            "description": "Open IA pipeline",
            "source": "CS Report",
            "query": (
                f"On-slide value {fmt_platform_value_dollar(total_open)} — sum of "
                "inventoryActionOpenValue endValue across customer week rows"
            ),
        },
        {
            "description": "Recs created (30d)",
            "source": "CS Report",
            "query": (
                f"On-slide value {fmt_platform_value_count(total_recs)} — sum of "
                "recsCreatedLast30DaysCt endValue across customer week rows"
            ),
        },
        {
            "description": "POs placed (30d)",
            "source": "CS Report",
            "query": f"Shown in gray subline as {total_pos:,} POs placed — sum of posPlacedInLast30DaysCt endValue across customer week rows",
        },
        {
            "description": "Overdue tasks",
            "source": "CS Report",
            "query": f"Shown in gray subline as {total_overdue:,} overdue tasks — sum of workbenchOverdueTasksCt endValue across customer week rows",
        },
        {
            "description": "Factory",
            "source": "CS Report",
            "query": f"Table column; {n_table} site row(s) with savings or recs; values from factoryName per week row",
        },
        {
            "description": "Savings",
            "source": "CS Report",
            "query": "Table column — per-site savings_current_period (same KPI field as headline Savings achieved)",
        },
        {
            "description": "Recs (30d)",
            "source": "CS Report",
            "query": "Table column — per-site recs_created_30d (same KPI field as headline Recs created (30d))",
        },
    ]


def support_health_exec_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Support Health Summary."""
    jira = report.get("jira")
    if not isinstance(jira, dict) or jira.get("error") or jira.get("total_issues", 0) == 0:
        return []
    total = jira["total_issues"]
    open_n = jira.get("open_issues", 0)
    escalated = jira.get("escalated", 0)
    ttfr = jira.get("ttfr", {})
    ttr = jira.get("ttr", {})
    rows: list[dict[str, str]] = [
        {
            "description": "Open tickets",
            "source": "Jira (HELP)",
            "query": f"On-slide value {open_n} — open issues in HELP project (of {total} total in period)",
        },
        {
            "description": "Escalated",
            "source": "Jira (HELP)",
            "query": f"On-slide value {escalated} — issues with escalation flag",
        },
        {
            "description": "TTFR (median)",
            "source": "Jira (HELP)",
            "query": f"On-slide value {ttfr.get('median', '—')} — median time to first response across {ttfr.get('measured', 0)} measured tickets",
        },
        {
            "description": "TTR (median)",
            "source": "Jira (HELP)",
            "query": f"On-slide value {ttr.get('median', '—')} — median time to resolution across {ttr.get('measured', 0)} measured tickets",
        },
    ]
    sentiment = jira.get("by_sentiment", {})
    if sentiment:
        parts = [f"{key}: {value}" for key, value in sentiment.items() if key != "Unknown"]
        rows.append({"description": "Sentiment", "source": "Jira (HELP)", "query": f"Ticket sentiment breakdown — {', '.join(parts)}"})
    return rows


def salesforce_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Salesforce Pipeline."""
    sf = report.get("salesforce")
    if not isinstance(sf, dict) or not sf.get("matched"):
        return []
    opp = sf.get("opportunity_count_this_year", 0)
    arr = sf.get("pipeline_arr", 0)
    n_accounts = len(sf.get("accounts", []))
    return [
        {"description": "Pipeline ARR", "source": "Salesforce", "query": f"On-slide value ${arr:,.0f} — sum of Amount on open Opportunity records for matched accounts"},
        {"description": "Opportunities (this year)", "source": "Salesforce", "query": f"On-slide value {opp} — count of Opportunity records with CloseDate in current fiscal year"},
        {"description": "SF accounts matched", "source": "Salesforce", "query": f"On-slide value {n_accounts} — Customer Entity accounts matched by name search"},
    ]


def platform_risk_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Platform Risk slide."""
    rows: list[dict[str, str]] = []
    csr = get_csr_section(report)
    cs_ph = csr.get("platform_health")
    if isinstance(cs_ph, dict) and not cs_ph.get("error"):
        dist = cs_ph.get("health_distribution", {})
        factory_count = cs_ph.get("factory_count", 0)
        rows.extend([
            {"description": "Factory count", "source": "CS Report", "query": f"On-slide value {factory_count} — number of factory rows in CS Report platform health"},
            {"description": "Health distribution", "source": "CS Report", "query": f"GREEN={dist.get('GREEN', 0)} YELLOW={dist.get('YELLOW', 0)} RED={dist.get('RED', 0)} — health_score band per factory"},
            {"description": "Critical shortages", "source": "CS Report", "query": f"On-slide value {cs_ph.get('total_critical_shortages', 0)} — sum of criticalShortagesCt across factories"},
        ])
    cs_sc = csr.get("supply_chain")
    if isinstance(cs_sc, dict) and not cs_sc.get("error"):
        totals = cs_sc.get("totals", {})
        rows.extend([
            {"description": "Total on-hand", "source": "CS Report", "query": f"On-slide value ${totals.get('total_on_hand', 0):,.0f} — sum of onHandValue across factories"},
            {"description": "Excess inventory", "source": "CS Report", "query": f"On-slide value ${totals.get('total_excess', 0):,.0f} — sum of excessValue across factories"},
            {"description": "Late POs", "source": "CS Report", "query": f"On-slide value {totals.get('total_late_pos', 0):,} — sum of latePosCt across factories"},
        ])
    return rows


def cohort_summary_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Cohort Summary."""
    metrics = _cohort_summary_metrics(report)
    if not metrics:
        return []
    labels = _CohortSummaryLabels
    total_arr = metrics["total_arr"]
    arr_echo = f"${total_arr:,.0f}" if total_arr > 0 else "—"
    med_login = metrics["med_login"]
    med_write = metrics["med_write"]
    med_exports = metrics["med_exports"]
    med_kei = metrics["med_kei"]
    return [
        {"description": labels.TOTAL_CUSTOMERS, "source": "Pendo", "query": f"On-slide value {metrics['total_customers']} — customer_count in portfolio report (cohort_digest scope)"},
        {"description": labels.COHORTS, "source": "Pendo", "query": f"On-slide value {metrics['num_cohorts']} — cohort buckets with ≥1 customer (get_customer_cohort / cohorts.yaml)"},
        {
            "description": labels.TOTAL_ARR,
            "source": "Salesforce",
            "query": (
                f"On-slide value {arr_echo} — sum of Account ARR__c for matched customers "
                f"(Name / LeanDNA_Entity_Name__c / Parent / Ultimate Parent); "
                f"{len(report.get('_arr_by_customer') or {})} accounts matched"
            ),
        },
        {"description": labels.TOTAL_USERS, "source": "Pendo", "query": f"On-slide value {metrics['total_users']:,} — sum of total_users across cohort_digest buckets"},
        {"description": labels.ACTIVE_USERS_7D, "source": "Pendo", "query": f"On-slide value {metrics['total_active']:,} — sum of total_active_users (7d) across cohort_digest buckets"},
        {"description": labels.ACTIVE_RATE, "source": "Pendo", "query": f"On-slide value {metrics['overall_active_pct']}% — 100 × active_users / total_users (portfolio-wide across cohorts)"},
        {
            "description": labels.WEEKLY_ACTIVE_MEDIAN,
            "source": "Pendo",
            "query": f"On-slide value {med_login}% — median of per-cohort median_login_pct (each cohort median is across its customers’ engagement.active_rate_7d)" if med_login is not None else "On-slide — — median of per-cohort median_login_pct (insufficient data)",
        },
        {
            "description": labels.WRITE_RATIO_MEDIAN,
            "source": "Pendo",
            "query": f"On-slide value {med_write}% — median of per-cohort median_write_ratio (depth.write_ratio per customer, median within cohort, then median across cohorts)" if med_write is not None else "On-slide — — median of per-cohort write ratios (insufficient data)",
        },
        {
            "description": labels.KEI_ADOPTION_MEDIAN,
            "source": "Pendo",
            "query": f"On-slide value {med_kei}% — median of per-cohort kei_adoption_pct (% of customers in bucket with ≥1 Kei query)" if med_kei is not None else "On-slide — — median of per-cohort Kei adoption (insufficient data)",
        },
        {
            "description": labels.EXPORTS_MEDIAN,
            "source": "Pendo",
            "query": f"On-slide value {med_exports:.0f} — median of per-cohort median_exports (exports.total_exports per customer, 30d window, median within cohort then across cohorts)" if med_exports is not None else "On-slide — — median of per-cohort export medians (insufficient data)",
        },
        {"description": labels.LARGEST_COHORT, "source": "Pendo", "query": f"On-slide value {metrics['biggest_lbl']} — cohort_digest bucket with max customer count"},
    ]


def cohort_profile_pipeline_rows_for_block(
    report: dict[str, Any],
    block: dict[str, Any],
    *,
    cohort_label: str,
) -> list[dict[str, str]]:
    """One trace line per on-slide metric for a single cohort bucket."""
    labels = _CohortProfileTraceLabels
    name = block.get("display_name", cohort_label)
    n = int(block.get("n") or 0)
    total_active = int(block.get("total_active_users") or 0)
    total_users = int(block.get("total_users") or 0)
    median_login = block.get("median_login_pct")
    median_write = block.get("median_write_ratio")
    kei_pct = block.get("kei_adoption_pct", 0)
    median_exports = block.get("median_exports")
    median_login_os = "On-slide —" if median_login is None else f"On-slide {median_login}%"
    median_write_os = "On-slide —" if median_write is None else f"On-slide {median_write}%"
    median_exports_os = "On-slide —" if median_exports is None else f"On-slide {median_exports:.0f}"

    rows = [
        {"description": f"Cohort profile: {name} ({n} customers)", "source": "Pendo", "query": f"Bucket {cohort_label!r} in cohort_digest — get_customer_cohort / cohorts.yaml; portfolio rollup customer summaries"},
        {"description": labels.ACTIVE_USERS_7D, "source": "Pendo", "query": f"On-slide cohort total {total_active:,} — cohort_digest.total_active_users ({name})"},
        {"description": labels.TOTAL_USERS, "source": "Pendo", "query": f"On-slide cohort total {total_users:,} — cohort_digest.total_users ({name})"},
        {"description": labels.WEEKLY_ACTIVE_MEDIAN, "source": "Pendo", "query": f"{median_login_os} — median of engagement.active_rate_7d across customers in this cohort ({name})"},
        {"description": labels.WRITE_RATIO_MEDIAN, "source": "Pendo", "query": f"{median_write_os} — median of depth.write_ratio per customer in cohort ({name})"},
        {"description": labels.KEI_ADOPTERS_PCT, "source": "Pendo", "query": f"On-slide {kei_pct}% — share of customers in cohort with ≥1 Kei query ({name})"},
        {"description": labels.EXPORTS_MEDIAN, "source": "Pendo", "query": f"{median_exports_os} — median exports.total_exports (30d) per customer in cohort ({name})"},
    ]

    arr_map = report.get("_arr_by_customer") or {}
    customers = block.get("customers") or []
    cohort_arr = sum(float(arr_map.get(customer, 0) or 0) for customer in customers)
    n_matched = sum(1 for customer in customers if float(arr_map.get(customer, 0) or 0) > 0)
    if cohort_arr > 0:
        rows.append({
            "description": labels.TOTAL_ARR,
            "source": "Salesforce",
            "query": (
                f"On-slide {fmt_platform_value_dollar(cohort_arr)} — sum Account.ARR__c for "
                f"{n_matched}/{len(customers)} cohort customers with matches ({name}); "
                "Name / LeanDNA_Entity_Name__c / Parent / Ultimate Parent match"
            ),
        })
    return rows


def cohort_profiles_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Cohort Profile slide(s)."""
    entry = report.get("_speaker_note_slide_entry")
    entry = entry if isinstance(entry, dict) else {}
    scoped = entry.get("_cohort_profile_block")
    if isinstance(scoped, dict) and int(scoped.get("n") or 0) > 0:
        cohort_id = str(scoped.get("cohort_id") or scoped.get("display_name") or "bucket")
        return cohort_profile_pipeline_rows_for_block(report, scoped, cohort_label=cohort_id)

    digest = report.get("cohort_digest") or {}
    if not digest:
        return []
    rows: list[dict[str, str]] = []
    ordered = sorted(
        digest.items(),
        key=lambda kv: (kv[0] == "unclassified", -int((kv[1] or {}).get("n") or 0) if isinstance(kv[1], dict) else 0),
    )
    for cohort_id, block in ordered:
        if not isinstance(block, dict) or not int(block.get("n") or 0):
            continue
        rows.extend(cohort_profile_pipeline_rows_for_block(report, block, cohort_label=str(cohort_id)))

    arr_map = report.get("_arr_by_customer") or {}
    if arr_map:
        n_with = len(arr_map)
        total_arr = sum(arr_map.values())
        rows.append({
            "description": "ARR by customer (portfolio)",
            "source": "Salesforce (Account.ARR__c)",
            "query": (
                f"Matched {n_with} customers with ARR totalling ${total_arr:,.0f} — "
                "single batch query on Entity accounts, matched by Name / LeanDNA_Entity_Name__c / Parent / Ultimate Parent"
            ),
        })
    return rows


def cohort_findings_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Cohort Findings slide."""
    bullets = report.get("cohort_findings_bullets") or []
    if not bullets:
        return []
    min_customers = cohort_findings_min_customers_for_cross_cohort_compare()
    return [{
        "description": "Cohort findings",
        "source": "Pendo (compute_cohort_portfolio_rollup)",
        "query": (
            f"{len(bullets)} bullet(s): portfolio totals, per-cohort medians (login, write, exports, Kei), "
            f"cross-cohort spreads (cohorts with n≥{min_customers} only; slides/cohort-02-findings.yaml rollup_params) — "
            "from full portfolio customer summaries in this report"
        ),
    }]


def cs_notable_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    source = (report.get("support_notable_bullets_source") or "").strip() or "static"
    return [{
        "description": "Notable (CS) bullets",
        "source": "LLM" if source == "llm" else "static / YAML (digest + optional LLM in support deck run)",
        "query": f"source={source}; Jira + engagement digest; see BPO logs for this run",
    }]


CANONICAL_PIPELINE_TRACES: dict[str, Any] = {
    "health": health_snapshot_pipeline_traces,
    "benchmarks": peer_benchmarks_pipeline_traces,
    "platform_value": platform_value_pipeline_traces,
    "support_health_exec": support_health_exec_pipeline_traces,
    "salesforce_pipeline": salesforce_pipeline_traces,
    "platform_risk": platform_risk_pipeline_traces,
    "cohort_summary": cohort_summary_pipeline_traces,
    "cohort_profiles": cohort_profiles_pipeline_traces,
    "cohort_findings": cohort_findings_pipeline_traces,
    "cs_notable": cs_notable_pipeline_traces,
}


def build_slide_jql_speaker_notes_for_entry(
    report: dict[str, Any],
    entry: dict[str, Any],
    *,
    data_requirements: dict[str, list[str]],
) -> str:
    """Build speaker notes for one slide-plan entry using slide registries."""
    return build_slide_jql_speaker_notes(
        report,
        entry,
        data_requirements=data_requirements,
        canonical_pipeline_traces=CANONICAL_PIPELINE_TRACES,
    )
