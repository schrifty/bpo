"""Account Health Snapshot scoring and slide builder."""

from __future__ import annotations

from typing import Any

from .cs_report_client import get_csr_section
from .slide_loader import benchmarks_min_peers_for_cohort_median
from .slide_primitives import pill as _pill, slide_title as _slide_title, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import (
    BLUE,
    BODY_Y,
    CONTENT_W,
    FONT,
    GRAY,
    MARGIN,
    NAVY,
    SLIDE_W,
    TITLE_Y,
    WHITE,
    _HealthSnapshotLabels,
)

_HEALTH_GOOD = {"red": 0.10, "green": 0.55, "blue": 0.28}  # green
_HEALTH_MOD = BLUE  # blue
_HEALTH_BAD = {"red": 0.78, "green": 0.18, "blue": 0.18}  # red
_HEALTH_NA = GRAY  # no data

_SCORE_MAP = {"HEALTHY": 3, "MODERATE": 2, "AT RISK": 1}
_LABEL_FROM_SCORE = {3: "HEALTHY", 2: "MODERATE", 1: "AT RISK"}
_COLOR_FROM_LABEL = {"HEALTHY": _HEALTH_GOOD, "MODERATE": _HEALTH_MOD, "AT RISK": _HEALTH_BAD}


def score_engagement(report: dict[str, Any]) -> tuple[str, str]:
    """Score user-engagement health from Pendo active rate. Returns (label, rationale)."""
    rate = report.get("engagement", {}).get("active_rate_7d", 0)
    if rate >= 40:
        return "HEALTHY", f"{rate}% weekly active"
    if rate >= 20:
        return "MODERATE", f"{rate}% weekly active"
    return "AT RISK", f"{rate}% weekly active"


def score_platform(report: dict[str, Any]) -> tuple[str, str] | None:
    """Score platform health from CS Report factory health scores. Returns None if no data."""
    cs = get_csr_section(report).get("platform_health") or {}
    sites = cs.get("sites", [])
    if not sites:
        return None
    dist = cs.get("health_distribution", {})
    reds = dist.get("RED", 0)
    greens = dist.get("GREEN", 0)
    total = len(sites)
    pct_green = greens / max(total, 1) * 100
    if reds > 0:
        return "AT RISK", f"{reds} RED factory{'s' if reds != 1 else ''}"
    if pct_green >= 50:
        return "HEALTHY", f"{greens}/{total} factories GREEN"
    return "MODERATE", f"{greens}/{total} factories GREEN"


def score_support(report: dict[str, Any]) -> tuple[str, str] | None:
    """Score support health from Jira ticket data. Returns None if no data."""
    jira = report.get("jira", {})
    if not jira or jira.get("error") or jira.get("total_issues", 0) == 0:
        return None
    total = jira["total_issues"]
    escalated = jira.get("escalated", 0)
    open_n = jira.get("open_issues", 0)
    ttr = jira.get("ttr", {})
    breached = ttr.get("breached", 0)

    esc_pct = escalated / max(total, 1) * 100
    open_pct = open_n / max(total, 1) * 100

    if breached > 0 or esc_pct > 40:
        return "AT RISK", f"{escalated} escalated, {breached} SLA breach{'es' if breached != 1 else ''}"
    if esc_pct > 20 or open_pct > 50:
        return "MODERATE", f"{open_n} open, {escalated} escalated"
    return "HEALTHY", f"{open_n} open, {escalated} escalated"


def composite_health(report: dict[str, Any]) -> dict[str, Any]:
    """Compute composite health from all available dimensions."""
    dims: list[dict[str, Any]] = []

    eng_label, eng_why = score_engagement(report)
    dims.append({
        "name": "Engagement",
        "label": eng_label,
        "detail": eng_why,
        "source": "Pendo",
        "color": _COLOR_FROM_LABEL[eng_label],
    })

    plat = score_platform(report)
    if plat:
        dims.append({
            "name": "Platform",
            "label": plat[0],
            "detail": plat[1],
            "source": "CS Report",
            "color": _COLOR_FROM_LABEL[plat[0]],
        })

    supp = score_support(report)
    if supp:
        dims.append({
            "name": "Support",
            "label": supp[0],
            "detail": supp[1],
            "source": "Jira",
            "color": _COLOR_FROM_LABEL[supp[0]],
        })

    scores = [_SCORE_MAP[d["label"]] for d in dims]
    avg = sum(scores) / len(scores) if scores else 2
    if avg >= 2.5:
        overall = "HEALTHY"
    elif avg >= 1.5:
        overall = "MODERATE"
    else:
        overall = "AT RISK"

    return {
        "overall": overall,
        "overall_color": _COLOR_FROM_LABEL[overall],
        "dimensions": dims,
    }


def health_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Account Health Snapshot")

    eng = report["engagement"]
    bench = report["benchmarks"]
    acct = report["account"]
    rate = eng["active_rate_7d"]
    active = eng["active_7d"] + eng["active_30d"]
    internal = acct.get("internal_visitors", 0)

    health = composite_health(report)
    label = health["overall"]
    badge_bg = health["overall_color"]
    _pill(reqs, f"{sid}_badge", sid, SLIDE_W - MARGIN - 110, TITLE_Y + 2, 110, 28, label, badge_bg, WHITE)

    cohort_name = bench.get("cohort_name", "")
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    if cohort_med is not None and cohort_n >= benchmarks_min_peers_for_cohort_median():
        vs = rate - cohort_med
        direction = "above" if vs > 0 else "below" if vs < 0 else "at"
        bench_label = f"{cohort_name} median of {cohort_med}%  ({cohort_n} peers)"
    else:
        vs = rate - bench["peer_median_rate"]
        direction = "above" if vs > 0 else "below" if vs < 0 else "at"
        bench_label = f"all-customer median of {bench['peer_median_rate']}%  ({bench['peer_count']} peers)"
    labels = _HealthSnapshotLabels
    lines = [
        f"{labels.CUSTOMER_USERS}: {acct['total_visitors']}",
        f"{labels.ACTIVE_THIS_WEEK}: {eng['active_7d']}  ({rate}%)",
        f"{labels.ACTIVE_THIS_MONTH}: {active}",
        f"{labels.DORMANT}: {eng['dormant']}",
        "",
        f"{labels.WEEKLY_ACTIVE_RATE}: {rate}%  ({abs(vs):.0f}pp {direction} {bench_label})",
        f"{labels.SITES}: {acct['total_sites']}  |  {labels.COHORT}: {cohort_name or 'Unclassified'}",
    ]
    if internal:
        lines.append(f"({internal} internal staff excluded)")
    kpi = "\n".join(lines)

    _box(reqs, f"{sid}_kpi", sid, MARGIN, BODY_Y, CONTENT_W // 2 + 20, 200, kpi)
    _style(reqs, f"{sid}_kpi", 0, len(kpi), size=12, color=NAVY, font=FONT)

    offset = 0
    for line in lines:
        if ":" in line and line.strip() and not line.startswith("("):
            colon = line.index(":")
            _style(reqs, f"{sid}_kpi", offset, offset + colon + 1, bold=True)
        offset += len(line) + 1

    dims = health["dimensions"]
    dx = MARGIN + CONTENT_W // 2 + 40
    dw = CONTENT_W // 2 - 40
    dy = BODY_Y + 4

    for index, dimension in enumerate(dims):
        dot = "\u25cf"
        dim_line = f"{dot}  {dimension['name']}: {dimension['label']}"
        oid = f"{sid}_d{index}"
        _box(reqs, oid, sid, dx, dy, dw, 18, dim_line)
        _style(reqs, oid, 0, len(dim_line), bold=True, size=11, color=dimension["color"], font=FONT)

        detail = f"     {dimension['detail']}  ({dimension['source']})"
        did = f"{sid}_dd{index}"
        _box(reqs, did, sid, dx, dy + 16, dw, 14, detail)
        _style(reqs, did, 0, len(detail), size=9, color=GRAY, font=FONT)

        dy += 44

    return idx + 1
