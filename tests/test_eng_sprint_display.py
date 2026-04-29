"""Sprint label normalization for engineering portfolio slides."""

from src.slide_engineering_portfolio import _format_sprint_name_for_display


def test_format_sprint_name_inserts_space_before_version_digits():
    assert _format_sprint_name_for_display("Sprint590") == "Sprint 590"
    assert _format_sprint_name_for_display("sprint42") == "sprint 42"
    assert _format_sprint_name_for_display("Sprint 590") == "Sprint 590"
    assert _format_sprint_name_for_display("") == ""
    assert _format_sprint_name_for_display("  ") == ""
