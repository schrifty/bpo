"""Peer Benchmarks slide builder."""

from __future__ import annotations

from typing import Any

from .slide_loader import benchmarks_min_peers_for_cohort_median
from .slide_primitives import (
    kpi_metric_card as _kpi_metric_card,
    slide_title as _slide_title,
    style as _style,
)
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import (
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    FONT,
    MARGIN,
    _truncate_kpi_card_label,
)


def benchmarks_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Peer Benchmarks")

    bench = report["benchmarks"]
    acct = report["account"]
    cust_rate = bench["customer_active_rate"]
    all_med = bench["peer_median_rate"]
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    cohort_name = bench.get("cohort_name", "")
    use_cohort = cohort_med is not None and cohort_n >= benchmarks_min_peers_for_cohort_median()
    med_rate = cohort_med if use_cohort else all_med
    delta = cust_rate - med_rate

    row_y = BODY_Y + 8
    card_h = 58
    col_gap = 18.0
    n_cards = 3 if use_cohort else 2
    card_w = (CONTENT_W - (n_cards - 1) * col_gap) / n_cards

    _kpi_metric_card(
        reqs,
        f"{sid}_k0",
        sid,
        MARGIN,
        row_y,
        card_w,
        card_h,
        "Weekly active rate (this account)",
        f"{cust_rate}%",
        accent=BLUE,
        value_pt=22,
    )

    if use_cohort:
        med_lbl = _truncate_kpi_card_label(f"{cohort_name} median ({cohort_n} accounts)")
    else:
        med_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
    _kpi_metric_card(
        reqs,
        f"{sid}_k1",
        sid,
        MARGIN + card_w + col_gap,
        row_y,
        card_w,
        card_h,
        med_lbl,
        f"{med_rate}%",
        accent=BLUE,
        value_pt=22,
    )

    if use_cohort:
        all_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
        _kpi_metric_card(
            reqs,
            f"{sid}_k2",
            sid,
            MARGIN + 2 * (card_w + col_gap),
            row_y,
            card_w,
            card_h,
            all_lbl,
            f"{all_med}%",
            accent=BLUE,
            value_pt=22,
        )

    # Context narrative sits outside the KPI cards.
    peer_label = cohort_name if use_cohort else "peer"
    lines = [
        f"Delta: {'+' if delta >= 0 else ''}{delta:.0f} percentage points vs {peer_label} median",
        f"Account size: {acct['total_visitors']} users across {acct['total_sites']} sites",
        "",
    ]
    if delta > 15:
        lines.append(f"Engagement significantly exceeds {peer_label} average.")
        lines.append("Strong candidate for case study, reference, or expansion.")
    elif delta > 0:
        lines.append(f"Performing above {peer_label} average.")
        lines.append("Continue strategy; watch for expansion signals.")
    elif delta > -10:
        lines.append(f"Near the {peer_label} average.")
        lines.append("Monitor for downward trend; proactive outreach recommended.")
    else:
        lines.append(f"Significantly below {peer_label} average.")
        lines.append("Recommend re-engagement, executive check-in, training refresh.")

    ctx = "\n".join(lines)
    ctx_y = row_y + card_h + 16
    ctx_h = max(96.0, BODY_BOTTOM - ctx_y - 4)
    _box(reqs, f"{sid}_ctx", sid, MARGIN, ctx_y, CONTENT_W, ctx_h, ctx)
    _style(reqs, f"{sid}_ctx", 0, len(ctx), size=11, color=BLUE, font=FONT)

    return idx + 1
