"""Pagination registry and list-height helpers for slide builders."""

from src.slides_client import (
    BODY_BOTTOM,
    CONTENT_W,
    MARGIN,
    SLIDE_PAGINATING_SLIDE_TYPES,
    _exports_slide,
    _list_data_rows_fit_span,
    _single_embedded_chart_layout,
    slide_type_may_paginate,
)


def test_slide_type_may_paginate_features_and_title():
    assert slide_type_may_paginate("features") is True
    assert slide_type_may_paginate("title") is False


def test_all_paginating_types_are_strings():
    assert SLIDE_PAGINATING_SLIDE_TYPES
    assert all(isinstance(x, str) and x for x in SLIDE_PAGINATING_SLIDE_TYPES)


def test_list_rows_fit_span_reasonable_for_feature_slide_body():
    # BODY_Y=80, BODY_BOTTOM=369 → 289pt; line ~15pt → ~19 lines − 1 header ≈ 18
    n = _list_data_rows_fit_span(
        y_top=80,
        y_bottom=369,
        font_body_pt=12,
        reserved_header_lines=1,
        max_rows_cap=30,
    )
    assert n >= 10
    assert n <= 22


def test_list_rows_respects_cap():
    n = _list_data_rows_fit_span(
        y_top=0,
        y_bottom=1000,
        font_body_pt=12,
        reserved_header_lines=1,
        max_rows_cap=7,
    )
    assert n == 7


def test_single_embedded_chart_layout_donut_is_square_and_centered():
    chart_top = 104.0
    x, y, w, h = _single_embedded_chart_layout(
        y_top=chart_top, bottom_pad=10, pie_or_donut=True,
    )
    assert abs(w - h) < 1e-6
    assert x >= MARGIN - 1e-6
    assert x + w <= MARGIN + CONTENT_W + 1e-6
    assert y >= chart_top - 1e-6
    assert y + h <= BODY_BOTTOM - 10 + 1e-6
    mid_content = MARGIN + CONTENT_W / 2
    assert abs((x + w / 2) - mid_content) < 1.0


def test_single_embedded_chart_layout_bar_uses_full_content_width():
    chart_top = 104.0
    x, y, w, h = _single_embedded_chart_layout(
        y_top=chart_top, bottom_pad=10, pie_or_donut=False,
    )
    assert abs(w - CONTENT_W) < 1e-6
    assert abs(x - MARGIN) < 1e-6
    assert h > 200


def test_exports_single_slide_when_exporters_fit_double_line_budget():
    """Top Exporters uses 2 lines per user; old max_exporters heuristic split at 8 users."""
    report = {
        "exports": {
            "by_feature": [{"feature": f"f{i}", "exports": i + 1} for i in range(5)],
            "top_exporters": [
                {"email": f"user{i}@example.com", "role": "Buyer", "exports": i + 1}
                for i in range(8)
            ],
            "total_exports": 100,
            "exports_per_active_user": 2,
            "active_users": 10,
        }
    }
    reqs: list = []
    _exports_slide(reqs, "exp_test_sid", report, 0)
    assert sum(1 for r in reqs if "createSlide" in r) == 1
