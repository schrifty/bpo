"""Slide legends beside embedded Sheets pie charts must use PIE_SLICE_COLORS."""

from src.charts import PIE_SLICE_COLORS, pie_chart_slide_legend_entries


def test_pie_chart_slide_legend_entries_aligns_with_default_slice_palette():
    labels = ["Read", "Write", "Collab"]
    entries = pie_chart_slide_legend_entries(labels)
    assert len(entries) == 3
    for i, (lab, color) in enumerate(entries):
        assert lab == labels[i]
        assert color == PIE_SLICE_COLORS[i % len(PIE_SLICE_COLORS)]


def test_pie_chart_slide_legend_entries_cycles_long_label_lists():
    labels = [f"S{i}" for i in range(len(PIE_SLICE_COLORS) + 2)]
    entries = pie_chart_slide_legend_entries(labels)
    assert entries[-1][1] == PIE_SLICE_COLORS[1]
