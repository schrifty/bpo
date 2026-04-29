"""Helpers for engineering portfolio ticket subject + description lines."""

from src.slide_engineering_portfolio import _first_two_description_lines, _truncate_one_line


def test_truncate_one_line():
    assert _truncate_one_line("hello", 10) == "hello"
    assert _truncate_one_line("abcdefghij", 5) == "abcd…"


def test_first_two_description_lines_word_split():
    body = "one two three four five six seven eight nine ten"
    a, b = _first_two_description_lines(body, line_chars=12)
    assert "one" in a
    assert len(a) <= 12
    assert b
    assert "…" in b or len(b) <= 12
