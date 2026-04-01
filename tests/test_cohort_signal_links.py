"""Helpers for linking Notable Signals phrasing to the cohort review deck."""

from src.slides_client import (
    _slides_shape_text_plain,
    _utf16_code_unit_len,
    _utf16_ranges_for_phrases,
)


def test_utf16_ranges_find_cohort_and_portfolio_phrases():
    full = "1.   Strong: 50% weekly active rate vs 40% cohort median\n2.   Other\n"
    r = _utf16_ranges_for_phrases(full, ("cohort median", "portfolio median"))
    assert len(r) == 1
    start, end = r[0]
    j = full.index("cohort median")
    assert start == _utf16_code_unit_len(full[:j])
    assert end - start == _utf16_code_unit_len("cohort median")


def test_utf16_ranges_non_ascii_prefix():
    phrase = "cohort median"
    full = f"Bullet: café vs {phrase}"
    r = _utf16_ranges_for_phrases(full, (phrase,))
    assert len(r) == 1
    s, e = r[0]
    assert s == _utf16_code_unit_len("Bullet: café vs ")
    assert e - s == _utf16_code_unit_len(phrase)


def test_slides_shape_text_plain_concatenates_runs():
    tb = {
        "textElements": [
            {"textRun": {"content": "a"}},
            {"textRun": {"content": "b"}},
        ]
    }
    assert _slides_shape_text_plain(tb) == "ab"
