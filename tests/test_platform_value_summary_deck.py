"""Platform Value & ROI Summary deck — cover and TOC builders."""

from src.slide_platform_value import platform_value_summary_toc_slide


def test_platform_value_summary_toc_lists_slides_excluding_cover_and_toc():
    plan = [
        {"slide_type": "platform_value_summary_cover", "title": "Cover"},
        {"slide_type": "platform_value_summary_toc", "title": "TOC"},
        {"slide_type": "platform_value", "title": "Platform Value & ROI"},
        {"slide_type": "supply_chain", "title": "Supply Chain"},
    ]
    report = {"_slide_plan": plan, "_current_slide": {"title": "Table of Contents"}}
    reqs: list = []
    out = platform_value_summary_toc_slide(reqs, "pv_toc", report, 0)
    assert out == 1
    texts = []
    for r in reqs:
        ins = r.get("insertText")
        if ins and isinstance(ins.get("text"), str):
            texts.append(ins["text"])
    joined = " ".join(texts)
    assert "Platform Value & ROI" in joined
    assert "Supply Chain" in joined
    assert "Cover" not in joined
    assert "TOC" not in joined
