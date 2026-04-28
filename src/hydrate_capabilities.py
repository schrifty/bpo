"""Capability metadata used by hydrate slide analysis and classification."""

from __future__ import annotations

from .slides_client import get_slide_data_requirements, slide_builder_names


# Canonical data keys we can resolve from report/data_summary. LLM uses these or adds slugs.
CANONICAL_DATA_KEYS = (
    "customer_name", "report_date", "quarter", "quarter_start", "quarter_end",
    "total_users", "total_visitors", "unique_visitors", "active_users",
    "total_sites", "active_sites", "health_score",
    "account_total_minutes", "account_avg_weekly_hours",
    "total_shortages", "total_critical_shortages", "weekly_active_buyers_pct_avg",
    "site_details", "cs_health_sites", "support", "salesforce", "platform_value",
    "supply_chain",
)

AVAILABLE_DATA_KEYS = frozenset(CANONICAL_DATA_KEYS)

DATA_SOURCES: dict[str, list[str]] = {
    "Pendo": [
        "engagement tiers (power/core/casual/dormant)", "active user counts & rates (7d/30d)",
        "page views & feature usage ranked", "visitor roles & departments",
        "champion (most active) & at-risk (dormant) user lists with emails",
        "site-level metrics (visitors, events, minutes, last-active)",
        "export behavior (counts by feature, by user, top exporters)",
        "Kei AI chatbot adoption & executive usage",
        "guide engagement (seen/dismissed/advanced rates, per-guide)",
        "customer list with sizing & activity ranking",
        "behavioral depth (read/write/collab breakdown by feature category)",
        "cohort benchmarking (median active rates by manufacturing vertical)",
    ],
    "Jira / JSM": [
        "HELP project tickets (open/resolved/total, by priority & status)",
        "SLA metrics (TTFR, TTR - median & average, breach rate, % measured)",
        "ticket sentiment (positive/neutral/negative/unrated)",
        "request channel mix (portal/email/internal)",
        "LEAN project engineering pipeline (open/shipped by priority)",
        "ER project enhancement requests (open/shipped/declined, by priority)",
    ],
    "CS Report (Google Sheets export)": [
        "health status (GREEN/YELLOW/RED) per customer/site",
        "CTB%, CTC%, component availability",
        "shortage counts per site",
        "inventory values, days of inventory (DOI), excess inventory",
        "late PO counts & values",
        "savings achieved, open intelligent-action value",
        "recommendations created, POs placed",
        "daily & weekly active buyer counts & percentages",
    ],
    "teams.yaml (local config)": [
        "CSM / AE / SE team roster per customer (manually maintained)",
    ],
    "cohorts.yaml (local config)": [
        "manufacturing cohort classification per customer (e.g. Aerospace, Automotive)",
    ],
}

SLIDE_BUILDING_CAPABILITIES: list[str] = [
    "Text boxes — configurable font family, size, color, bold/italic, alignment",
    "Tables — header rows, per-cell background color, custom border weight, column widths",
    "Colored rectangles — metric cards, status badges, progress-bar fills",
    "Solid background fills on slides",
    "Two-column and multi-column layouts with precise pt positioning",
    "Dynamic content fitting within a protected BODY_BOTTOM margin",
    "Number formatting (abbreviation: 1.2M, $3.4K, 42.1%)",
    "Branded color palette (navy #081c33, blue #009aff, teal #38c0ce, mint #aefff6, etc.)",
    "Fonts: Source Sans Pro, IBM Plex Serif, Source Sans 3 (monospace)",
    "Auto-skip slides when data is empty (no half-blank slides)",
]

KNOWN_LIMITATIONS: list[str] = [
    "No embedded raster charts (bar, line, pie) — we build metric cards and tables instead. "
    "Matplotlib could render to PNG and be inserted as an image, but this is not wired up yet.",
    "No image insertion from external URLs or Drive (only text shapes, rectangles, and tables).",
    "No Salesforce data yet (ARR, renewal dates, contacts, opportunity pipeline — planned).",
    "No animations, transitions, or speaker notes.",
    "Fixed 720×405 pt (standard 16:9) slide canvas.",
    "No grouped/layered elements — every element is a flat shape on the slide.",
]

BUILDER_DESCRIPTIONS = {
    "qbr_cover": "Branded QBR cover — customer name, date, 'Executive business review'",
    "qbr_agenda": "Numbered agenda listing sections of the deck",
    "qbr_divider": "Section divider with LeanDNA tagline and a section title",
    "qbr_deployment": "Deployment overview — site count, health status, last active dates",
    "title": "Title slide with customer name, date range, CSM, site/user counts",
    "health": "Account health snapshot — engagement tiers, health score, benchmarks",
    "engagement": "Engagement breakdown — active/dormant counts by tier and role",
    "sites": "Site comparison table — users, pages, features, events per site",
    "features": "Feature adoption — top pages and features ranked by usage",
    "champions": "Champions & at-risk users — most active and dormant users with emails",
    "benchmarks": "Peer benchmarking — customer metrics vs cohort medians",
    "exports": "Export behavior — total exports, by feature, by user, top exporters",
    "depth": "Behavioral depth — read/write/collab breakdown by feature category",
    "kei": "Kei AI adoption — chatbot usage, adoption rate, executive engagement",
    "guides": "Guide engagement — onboarding guides seen/dismissed/advanced rates",
    "jira": "Support summary — HELP ticket counts, priority, status breakdown",
    "customer_ticket_metrics": "Per-customer HELP KPIs — open/resolved counts, SLA (median & average), type/status bar charts",
    "support_help_orgs_by_opened": (
        "All-customers only — table ranking JSM organizations by HELP tickets created in the last ~90 days"
    ),
    "support_help_customer_escalations": (
        "Open HELP issues with label customer_escalation and not Done, ordered by last update"
    ),
    "support_help_escalation_metrics": (
        "HELP only - open backlog TTR (median) with vs without label customer_escalation; "
        "counts of open escalations, created in 90d, resolved in 90d"
    ),
    "support_recent_opened": "HELP tickets opened in the last ~45 days for the scoped customer",
    "support_recent_closed": "HELP tickets resolved in the last ~45 days for the scoped customer",
    "sla_health": "Support health & SLA — TTFR/TTR, breach rate, sentiment, channels",
    "engineering": "Engineering pipeline — LEAN project open/shipped tickets",
    "enhancements": "Enhancement requests — ER project open/shipped/declined",
    "platform_health": "Platform health — CS Report health status, CTB%, CTC%, shortages",
    "supply_chain": "Supply chain — inventory values, DOI, excess, late POs",
    "platform_value": "Platform value & ROI — savings, IA value, recs created, POs placed",
    "cross_validation": "Data cross-validation — Pendo vs CS Report engagement comparison",
    "signals": "Notable signals — auto-detected churn risk, expansion, adoption gaps",
    "team": "Team roster — CSM/AE assignments from teams.yaml",
    "data_quality": "Data quality — cross-source validation results",
    "custom": "Static content slide — reproduced text with title and body sections",
    "skip": "Skip this slide entirely (blank, transition, or not reproducible)",
    "salesforce_comprehensive_cover": "Salesforce export intro — match status, row limits, org-wide product note",
    "salesforce_category": "Salesforce table — one object category (sf_category) from comprehensive fetch",
    "cohort_deck_title": "Cohort deck cover — portfolio period, customer count, cohorts.yaml reference",
    "cohort_profiles": "Per-cohort profile slides — medians and account list for each manufacturing cohort bucket",
    "cohort_findings": (
        "Single slide — bullet list comparing cohort buckets (sample sizes, median login/write, "
        "Kei adoption spread when ≥2 cohorts have enough accounts (default n≥5 per cohort_findings "
        "rollup_params in slides YAML), unclassified count); auto text from portfolio rollup — "
        "not per-account profiles (that's cohort_profiles) or risk signals (that's signals)"
    ),
    "pendo_sentiment": "Polls & NPS — response counts, medians, sample pollEvents",
    "pendo_friction": "UX friction dashboard — rage/dead/error/U-turn totals and top pages/features",
    "pendo_localization": "Visitor UI languages — distribution from metadata.agent.language",
    "pendo_track_analytics": "Custom pendo.track events — names, events, unique users",
    "pendo_definitions_appendix": "Appendix — sample segment/report/track type names (definitions only)",
}


def builder_descriptions_text() -> str:
    return "\n".join(f"  - {key}: {description}" for key, description in BUILDER_DESCRIPTIONS.items())


def existing_slide_types() -> list[str]:
    return sorted(slide_builder_names())


def build_capability_context() -> str:
    """Build a text summary of current hydrate capabilities for the LLM."""
    lines = ["# Current Capabilities\n"]

    lines.append("## Data Sources")
    for src, fields in DATA_SOURCES.items():
        lines.append(f"\n### {src}")
        for field in fields:
            lines.append(f"  - {field}")

    lines.append("\n## Slide Building")
    for cap in SLIDE_BUILDING_CAPABILITIES:
        lines.append(f"  - {cap}")

    lines.append("\n## Existing Slide Types")
    for slide_type in existing_slide_types():
        requirements = get_slide_data_requirements(slide_type)
        lines.append(f"  - {slide_type}: needs [{', '.join(requirements)}]")

    lines.append("\n## Known Limitations")
    for limitation in KNOWN_LIMITATIONS:
        lines.append(f"  - {limitation}")

    return "\n".join(lines)
