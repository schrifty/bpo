"""Slide dimensions, brand palette, and layout helpers shared by slides_client and charts."""

import datetime
from typing import Any

SLIDE_W = 720
SLIDE_H = 405

# Layout
MARGIN = 48
CONTENT_W = SLIDE_W - 2 * MARGIN
TITLE_Y = 28
BODY_Y = 80
BODY_BOTTOM = SLIDE_H - 36  # safe bottom edge (room for omission note + footer)

# Max physical slides one logical slide type may span when paginating (tables, lists, continuations).
MAX_PAGINATED_SLIDE_PAGES = 10

# ── Pagination registry & layout helpers ─────────────────────────────────────
# Use ``slide_type_may_paginate()`` for tooling/docs. Builders with ad-hoc continuation
# (e.g. ``jira``) still count as paginating when they emit ``_pN`` continuation slides.
SLIDE_PAGINATING_SLIDE_TYPES: frozenset[str] = frozenset({
    "sites",
    "features",
    "exports",
    "signals",
    "portfolio_signals",
    "portfolio_trends",
    "data_quality",
    "platform_health",
    "supply_chain",
    "platform_value",
    "cross_validation",
    "engineering",
    "jira",
    "enhancements",
    "eng_enhancements",
    "bespoke_deployment",
    "salesforce_category",
    "support_recent_opened",
    "support_recent_closed",
    "cohort_profiles",
})


def slide_type_may_paginate(slide_type: str) -> bool:
    """True if this ``slide_type`` can produce more than one physical slide."""
    return slide_type in SLIDE_PAGINATING_SLIDE_TYPES


def _estimated_body_line_height_pt(font_body_pt: float | int) -> int:
    """Approximate line height (pt) for multiline text boxes in Slides body text."""
    return max(11, int(round(float(font_body_pt) * 1.22)))


def _list_data_rows_fit_span(
    *,
    y_top: float,
    y_bottom: float,
    font_body_pt: float | int,
    reserved_header_lines: int = 1,
    max_rows_cap: int = 40,
) -> int:
    """How many list rows fit in ``[y_top, y_bottom]`` after a column header line."""
    line_h = _estimated_body_line_height_pt(font_body_pt)
    avail = max(0.0, float(y_bottom) - float(y_top))
    total_lines = int(avail // line_h)
    data_rows = total_lines - int(reserved_header_lines)
    return max(1, min(max_rows_cap, data_rows))


def _table_rows_fit_span(
    *,
    y_top: float,
    y_bottom: float,
    row_height_pt: float | int,
    reserved_table_rows: int = 2,
    max_rows_cap: int = 40,
) -> int:
    """How many **data** rows fit in a Slides table between ``y_top`` and ``y_bottom``.

    Use *row_height_pt* close to the **rendered** row height (font + Slides
    default cell padding), not the text ``fontSize`` alone.  Otherwise
    pagination packs too many rows and the table runs past ``BODY_BOTTOM``.
    *reserved_table_rows* is header plus worst-case footer (e.g. total row on
    the last page).
    """
    rh = max(12.0, float(row_height_pt))
    avail = max(0.0, float(y_bottom) - float(y_top))
    max_fit = int(avail // rh)
    data = max_fit - int(reserved_table_rows)
    return max(1, min(max_rows_cap, data))


def _single_embedded_chart_layout(
    *,
    y_top: float,
    bottom_pad: float = 10,
    pie_or_donut: bool,
) -> tuple[float, float, float, float]:
    """Return ``(x, y, w, h)`` in pt for one Sheets chart in the body band.

    Uses horizontal margins (``MARGIN`` / ``CONTENT_W``) and vertical space
    down to ``BODY_BOTTOM - bottom_pad``. Pie/donut charts use the largest
    centered square that fits; bar/line charts span full content width with
    nearly full band height, vertically centered.
    """
    avail_h = float(BODY_BOTTOM) - float(y_top) - float(bottom_pad)
    if avail_h < 30:
        avail_h = 80.0
    cw = float(CONTENT_W)
    if pie_or_donut:
        side = min(cw, avail_h) * 0.96
        w = h = side
        x = float(MARGIN) + (cw - w) / 2.0
        y = float(y_top) + max(0.0, (avail_h - h) / 2.0)
        return x, y, w, h
    w = cw
    h = avail_h * 0.96
    x = float(MARGIN)
    y = float(y_top) + max(0.0, (avail_h - h) / 2.0)
    return x, y, w, h


def _cap_page_count(n: int) -> int:
    return min(max(n, 1), MAX_PAGINATED_SLIDE_PAGES)


def _cap_chunk_list(chunks: list[Any]) -> list[Any]:
    if len(chunks) <= MAX_PAGINATED_SLIDE_PAGES:
        return chunks
    return chunks[:MAX_PAGINATED_SLIDE_PAGES]


# ── LeanDNA APEX brand palette (from template 1o2POERqEEp…) ──
NAVY = {"red": 0.031, "green": 0.110, "blue": 0.200}    # #081c33  dark navy
BLUE = {"red": 0.0,   "green": 0.604, "blue": 1.0}      # #009aff  primary accent
LTBLUE = {"red": 0.482, "green": 0.769, "blue": 0.980}   # #7bc4fa  secondary accent
TEAL = {"red": 0.220, "green": 0.753, "blue": 0.808}     # #38c0ce  tertiary accent
MINT = {"red": 0.682, "green": 1.0,   "blue": 0.965}     # #aefff6  highlight
WHITE = {"red": 1.0,  "green": 1.0,   "blue": 1.0}
DARK = NAVY                                                # alias for readability
GRAY = {"red": 0.522, "green": 0.522, "blue": 0.522}     # #858585  secondary text
BLACK = {"red": 0.0, "green": 0.0, "blue": 0.0}         # metric labels on LIGHT KPI tiles
# Universal KPI tile label size for ``_kpi_metric_card`` (keep in sync with SLIDE_DESIGN_STANDARDS.md).
KPI_METRIC_LABEL_PT = 10.0
LIGHT = {"red": 0.933, "green": 0.941, "blue": 0.953}    # #eef0f3  light background
FONT = "Source Sans Pro"
FONT_SERIF = "IBM Plex Serif"
MONO = "Source Sans 3"


# Account Health Snapshot — text before ':' must match _health_slide and speaker-note traces.
class _HealthSnapshotLabels:
    CUSTOMER_USERS = "Customer Users"
    ACTIVE_THIS_WEEK = "Active This Week"
    ACTIVE_THIS_MONTH = "Active This Month"
    DORMANT = "Dormant (30+ days)"
    WEEKLY_ACTIVE_RATE = "Weekly Active Rate"
    SITES = "Sites"
    COHORT = "Cohort"


# Cohort Summary KPI cards — labels must match _cohort_summary_slide and speaker-note traces.
# Cohort profile slide — trace ``description`` must match subtitle/stat wording on ``_cohort_profiles_slide``.
class _CohortProfileTraceLabels:
    ACTIVE_USERS_7D = "Active users (7d)"
    TOTAL_USERS = "Total users"
    WEEKLY_ACTIVE_MEDIAN = "Weekly active rate (median)"
    WRITE_RATIO_MEDIAN = "Write-to-total ratio (median)"
    KEI_ADOPTERS_PCT = "Kei adopters (% of customers)"
    EXPORTS_MEDIAN = "Exports per customer (median, 30d)"
    TOTAL_ARR = "Total ARR"


class _CohortSummaryLabels:
    TOTAL_CUSTOMERS = "Total customers"
    COHORTS = "Cohorts"
    TOTAL_ARR = "Total ARR"
    TOTAL_USERS = "Total users"
    ACTIVE_USERS_7D = "Active users (7d)"
    ACTIVE_RATE = "Active rate"
    WEEKLY_ACTIVE_MEDIAN = "Weekly active rate (median)"
    WRITE_RATIO_MEDIAN = "Write ratio (median)"
    KEI_ADOPTION_MEDIAN = "Kei adoption (median)"
    EXPORTS_MEDIAN = "Exports per customer (median, 30d)"
    LARGEST_COHORT = "Largest cohort"


def _cohort_summary_metrics(report: dict[str, Any]) -> dict[str, Any] | None:
    """Shared cohort summary numbers for ``_cohort_summary_slide`` and pipeline traces."""
    digest = report.get("cohort_digest") or {}
    buckets = [v for v in digest.values() if isinstance(v, dict) and int(v.get("n") or 0) > 0]
    if not buckets:
        return None
    total_customers = report.get("customer_count", 0)
    num_cohorts = len(buckets)
    total_users = sum(b.get("total_users", 0) for b in buckets)
    total_active = sum(b.get("total_active_users", 0) for b in buckets)
    overall_active_pct = round(100.0 * total_active / total_users, 1) if total_users else 0.0
    arr_map = report.get("_arr_by_customer") or {}
    total_arr = sum(arr_map.values()) if arr_map else 0.0
    login_medians = [b["median_login_pct"] for b in buckets if b.get("median_login_pct") is not None]
    write_medians = [b["median_write_ratio"] for b in buckets if b.get("median_write_ratio") is not None]
    export_medians = [b["median_exports"] for b in buckets if b.get("median_exports") is not None]
    kei_rates = [b.get("kei_adoption_pct", 0) for b in buckets]

    def _med(nums: list[Any]) -> float | int | None:
        if not nums:
            return None
        s = sorted(nums)
        mid = len(s) // 2
        return s[mid] if len(s) % 2 else round((s[mid - 1] + s[mid]) / 2, 1)

    med_login = _med(login_medians)
    med_write = _med(write_medians)
    med_exports = _med(export_medians)
    med_kei = _med(kei_rates)
    biggest = max(buckets, key=lambda b: b.get("n", 0))
    biggest_lbl = f"{biggest['display_name']} ({biggest['n']})"
    return {
        "buckets": buckets,
        "total_customers": total_customers,
        "num_cohorts": num_cohorts,
        "total_users": total_users,
        "total_active": total_active,
        "overall_active_pct": overall_active_pct,
        "total_arr": total_arr,
        "med_login": med_login,
        "med_write": med_write,
        "med_exports": med_exports,
        "med_kei": med_kei,
        "biggest_lbl": biggest_lbl,
    }


def _truncate_kpi_card_label(s: str, max_len: int = 44) -> str:
    """Truncate a KPI label to *max_len* characters.

    Prefer the width-aware logic inside ``_kpi_metric_card`` (which
    dynamically adjusts font size *and* truncates based on actual card
    geometry).  This function is a static safety net for call-sites that
    build labels before passing them to the card helper.
    """
    s = (s or "").strip()
    return s if len(s) <= max_len else f"{s[: max_len - 1]}…"


_KPI_LABEL_CHAR_WIDTH_FACTOR = 0.55
"""Approximate average character width as a fraction of font-size (pt)
for Roboto / sans-serif mixed-case text.  Conservative (slightly wide)
so truncation errs on the side of fitting."""

_KPI_LABEL_MIN_PT = 8.0
"""Smallest font we'll auto-shrink a KPI label to before truncating."""


def _fit_kpi_label(label: str, inner_w: float, label_pt: float) -> tuple[str, float]:
    """Return *(possibly truncated label, possibly reduced font size)*.

    Strategy: estimate rendered width from character count and font size.
    If the label is too wide, first try shrinking font (down to
    ``_KPI_LABEL_MIN_PT``).  If still too wide, truncate.
    """
    label = (label or "").strip()
    if not label:
        return label, label_pt

    def _max_chars(pt: float) -> int:
        return max(6, int(inner_w / (_KPI_LABEL_CHAR_WIDTH_FACTOR * pt)))

    if len(label) <= _max_chars(label_pt):
        return label, label_pt

    pt = label_pt
    while pt > _KPI_LABEL_MIN_PT:
        pt -= 0.5
        if len(label) <= _max_chars(pt):
            return label, pt

    mc = _max_chars(pt)
    return (f"{label[: mc - 1]}…", pt) if len(label) > mc else (label, pt)


def _date_range(days: int, quarter_label: str | None = None,
                quarter_start: str | None = None, quarter_end: str | None = None) -> str:
    """Format a human-readable date range, with optional quarter prefix.

    If quarter_label is set (e.g. 'Q1 2026'), the output looks like
    'Q1 2026 (Jan 1 – Mar 9, 2026)'.  Otherwise plain 'Feb 7 – Mar 9, 2026'.

    When quarter_start/quarter_end are provided (ISO date strings), they are
    used for display instead of computing from days, avoiding off-by-one errors.
    """
    if quarter_start and quarter_end:
        start = datetime.date.fromisoformat(quarter_start)
        end = datetime.date.fromisoformat(quarter_end)
    else:
        end = datetime.date.today()
        start = end - datetime.timedelta(days=days)
    if start.year == end.year:
        span = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
    else:
        span = f"{start.strftime('%b %-d, %Y')} – {end.strftime('%b %-d, %Y')}"
    if quarter_label:
        return f"{quarter_label} ({span})"
    return span

