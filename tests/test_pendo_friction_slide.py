"""pendo_friction body text must stay within estimated line budget."""

from src.slide_pendo import pendo_friction_slide
from src.slides_theme import BODY_BOTTOM, BODY_Y, _estimated_body_line_height_pt, _list_data_rows_fit_span


def _body_text_from_reqs(reqs: list) -> str:
    for r in reversed(reqs):
        ins = r.get("insertText")
        if not ins:
            continue
        t = ins.get("text")
        if isinstance(t, str) and "Total frustration signals" in t:
            return t
    raise AssertionError("no friction body insertText found")


def test_pendo_friction_caps_lines_when_many_pages_and_features():
    many_pages = [{"page": f"/page/{i}"} for i in range(50)]
    many_features = [{"feature": f"Feature {i}"} for i in range(50)]
    report = {
        "frustration": {
            "total_frustration_signals": 999_999,
            "totals": {
                "rageClickCount": 1,
                "deadClickCount": 2,
                "errorClickCount": 3,
                "uTurnCount": 4,
            },
            "top_pages": many_pages,
            "top_features": many_features,
        }
    }
    reqs: list = []
    pendo_friction_slide(reqs, "t_fr", report, 0)
    body = _body_text_from_reqs(reqs)
    line_count = body.count("\n") + (1 if body else 0)

    body_y = BODY_Y + 10
    body_bottom = BODY_BOTTOM - 4
    max_lines = _list_data_rows_fit_span(
        y_top=body_y,
        y_bottom=body_bottom,
        font_body_pt=11,
        reserved_header_lines=0,
        max_rows_cap=50,
    )
    assert line_count <= max_lines, (
        f"{line_count} lines vs budget {max_lines} "
        f"(line_h={_estimated_body_line_height_pt(11)})"
    )
