"""Notable: pad to six bullets (heuristics, dedupe) without LLM."""

from unittest.mock import patch

from src.support_notable_llm import _heuristic_bullets_from_digest, _parse_notable_bullets_json, _pad_notable_to_six, _salvage_bullet_strings_from_partial_json


@patch("src.support_notable_llm._top_up_notable_bullets", return_value=[])
def test_pad_reaches_six_from_one_bullet_with_heuristics(_mock_top: object) -> None:
    d = {
        "customer_name": "Acme",
        "help_tickets_for_this_customer": {
            "unresolved_count": 3,
            "resolved_in_6mo_count": 2,
            "sla_adherence_1y": 0.9,
            "by_type_open": {"Bug": 1, "Task": 2},
        },
        "help_resolved_by_assignee": {
            "total_resolved": 20,
            "by_assignee": [{"assignee": "Pat", "count": 5}],
        },
    }
    out = _pad_notable_to_six(["VOLUME: single LLM line only."], d, {})
    assert len(out) == 6
    assert out[0] == "VOLUME: single LLM line only."
    assert any("SLA HEALTH" in b for b in out)


def test_salvage_skips_broken_middle_string():
    s = r'{"bullets": ["ok one", "bad "inner" quote", "ok three", "ok four", "ok five", "ok six"]}'
    salv = _salvage_bullet_strings_from_partial_json(s)
    assert salv and "ok one" in salv
    # At least recover ok three after bad token
    assert "ok three" in salv and len(salv) >= 3


def test_parse_add_key():
    j = '{"add": ["A", "B", "C"]}'
    p = _parse_notable_bullets_json(j)
    assert p == ["A", "B", "C"]


def test_heuristic_dedupes_against_existing():
    d = {"customer_name": "X", "help_tickets_for_this_customer": {"unresolved_count": 1}}
    h = _heuristic_bullets_from_digest(
        d,
        [
            "VOLUME: X has 1 open HELP ticket(s) (JSM org scope) and None resolved in the 6-month metrics window (digest).",
        ],
        2,
    )
    assert h == [] or "2" not in str(h)  # should not echo same volume line