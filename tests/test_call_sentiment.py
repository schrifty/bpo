"""Call sentiment scoring from the Chorus meeting-summary sentence."""

from datetime import date
from pathlib import Path

import pytest

from src.healthscore_web.call_sentiment import (
    CallSentimentGeneratorError,
    backfill_call_sentiment,
    call_sentiment_points,
    get_call_sentiment,
    month_key,
    sentiment_clause_score,
    trailing_months,
)
from src.healthscore_web.framework import load_framework
from src.healthscore_web.store import connect, observations_for_metric

AS_OF = date(2026, 9, 28)
SITE_A = "001000000000001AAA"
SITE_B = "001000000000002AAA"
OTHER = "001000000000003AAA"
PARENT = "001000000000099AAA"
ENTITIES = [
    {"id": SITE_A, "name": "Site A"},
    {"id": SITE_B, "name": "Site B"},
    {"id": OTHER, "name": "Other"},
]
PARENTS = {SITE_A: PARENT, SITE_B: PARENT, OTHER: "001000000000088AAA"}


def _call(
    engagement_id: str,
    *,
    account: str = PARENT,
    when: str = "2026-09-10",
    kind: str = "meeting",
) -> dict:
    return {
        "engagement_id": engagement_id,
        "account_id": account,
        "engagement_type": kind,
        "no_show": False,
        "processing_state": "done",
        "duration": 600,
        "date_time": when,
    }


def _score(engagements: list[dict], summaries: dict[str, str], *, as_of: date = AS_OF) -> dict:
    result = get_call_sentiment(
        entities=ENTITIES,
        engagements=engagements,
        summaries=summaries,
        parent_ids=PARENTS,
        as_of=as_of,
        persist=False,
    )
    return {row["entity_id"]: row for row in result["readings"]}


def test_framework_call_sentiment_is_chorus_only() -> None:
    component = next(row for row in load_framework()["inputs"] if row["key"] == "call_sentiment")
    assert component["data_source"] == ["Chorus"]
    assert component["metric-generator"] == "get_call_sentiment"
    assert component["status"] == "defined"
    assert "Claude" not in component["description"]


def test_clause_scores_positive_negative_and_mixed() -> None:
    assert sentiment_clause_score("The overall sentiment was collaborative and pragmatic.") == 1
    assert sentiment_clause_score("Overall sentiment was frustrated and tense.") == -1
    assert sentiment_clause_score("Overall sentiment was collaborative yet concerned.") == 0
    assert sentiment_clause_score("Overall sentiment was professional.") == 0
    assert sentiment_clause_score("No sentiment sentence here.") is None
    assert sentiment_clause_score(None) is None


@pytest.mark.parametrize(
    ("value", "points"),
    [(None, None), (1, 2), (0.25, 2), (0, 1), (0.0, 1), (-0.5, 0), (-1, 0)],
)
def test_points_follow_the_sign_of_the_mean(value, points) -> None:
    assert call_sentiment_points(value) == points


def test_month_key_is_the_calendar_month() -> None:
    assert month_key(date(2026, 9, 28)) == "2026-09"
    assert trailing_months(date(2026, 9, 28), 6)[0] == date(2026, 4, 1)
    assert trailing_months(date(2026, 9, 28), 6)[-1] == date(2026, 9, 1)


def test_parent_account_credits_every_child_and_unmatched_stays_unscored() -> None:
    readings = _score(
        [
            _call("pos", when="2026-09-02"),
            _call("neg", when="2026-09-18"),
            _call("other", account=OTHER, when="2026-09-04"),
            _call("email", kind="email", when="2026-09-04"),
            _call("august", when="2026-08-20"),
        ],
        {
            "pos": "Overall sentiment was positive and constructive.",
            "neg": "Overall sentiment was negative.",
            "other": "Overall sentiment was collaborative.",
            "august": "Overall sentiment was positive.",
        },
    )
    shared = readings[SITE_A]
    assert shared["value"] == 0
    assert shared["points"] == 1
    assert shared["period_key"] == "2026-09"
    assert shared["meta"]["calls"] == 2
    assert shared["meta"]["positive"] == 1
    assert shared["meta"]["negative"] == 1
    assert readings[SITE_B]["value"] == 0
    assert readings[OTHER]["value"] == 1
    assert readings[OTHER]["points"] == 2


def test_missing_sentence_stays_unscored() -> None:
    readings = _score([_call("bare", account=OTHER)], {"bare": "A summary with no tone sentence."})
    assert readings[OTHER]["points"] is None
    assert readings[OTHER]["value"] is None
    assert "overall-sentiment" in readings[OTHER]["error"]


def test_backfill_scores_each_month_and_skips_one_already_stored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.config.CORTEX_CACHE_ROOT", tmp_path)
    conn = connect()
    from src.healthscore_web.store import upsert_reading

    upsert_reading(
        conn,
        entity_id=SITE_A,
        entity_name="Site A",
        metric_name="call_sentiment",
        grain="monthly",
        as_of="2026-09-30",
        period_key="2026-09",
        value=1,
        points=2,
        generator="manual-seed",
    )
    conn.close()
    engagements = [
        _call("aug", when="2026-08-12"),
        _call("sep", when="2026-09-12"),
    ]
    summaries = {
        "aug": "Overall sentiment was collaborative.",
        "sep": "Overall sentiment was frustrated.",
    }
    result = backfill_call_sentiment(
        months=2,
        entities=ENTITIES,
        as_of=AS_OF,
        engagements=engagements,
        summaries=summaries,
        parent_ids=PARENTS,
        persist=True,
    )
    assert result["months_skipped"] == ["2026-09"]
    assert [row["period_key"] for row in result["months"]] == ["2026-08"]
    assert result["months"][0]["scored"] == 2
    conn = connect()
    try:
        august = {
            row["entity_id"]: row
            for row in observations_for_metric(conn, SITE_A, "call_sentiment")
            if row["period_key"] == "2026-08"
        }
        september = observations_for_metric(conn, SITE_A, "call_sentiment")
    finally:
        conn.close()
    assert august[SITE_A]["points"] == 2
    assert any(row["period_key"] == "2026-09" and row["generator"] == "manual-seed" for row in september)


def test_points_reject_a_value_outside_the_scale() -> None:
    with pytest.raises(CallSentimentGeneratorError, match="-1 to \\+1"):
        call_sentiment_points(1.5)
