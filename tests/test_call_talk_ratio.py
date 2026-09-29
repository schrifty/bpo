"""Call talk-time scoring from Chorus rep and speaker talk seconds."""

from datetime import date
from pathlib import Path

import pytest

from src.healthscore_web.call_talk_ratio import (
    CallTalkRatioGeneratorError,
    backfill_call_talk_ratio,
    call_talk_ratio_points,
    get_call_talk_ratio,
    talk_seconds,
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


def _score(engagements: list[dict], shares: dict, *, as_of: date = AS_OF) -> dict:
    result = get_call_talk_ratio(
        entities=ENTITIES,
        engagements=engagements,
        shares=shares,
        parent_ids=PARENTS,
        as_of=as_of,
        persist=False,
    )
    return {row["entity_id"]: row for row in result["readings"]}


def test_framework_call_talk_ratio_is_chorus_only() -> None:
    component = next(row for row in load_framework()["inputs"] if row["key"] == "call_talk_ratio")
    assert component["data_source"] == ["Chorus"]
    assert component["metric-generator"] == "get_call_talk_ratio"
    assert component["status"] == "defined"
    assert component["target"] == 50
    assert component["target_direction"] == "lower"


def test_cluster_talk_time_counts_reps_against_every_speaker() -> None:
    body = {
        "data": {
            "attributes": {
                "recording": {
                    "clusters": [
                        {"type": "rep", "ranges": [{"talk_time": 30}, {"talk_time": 10}]},
                        {"type": "cust", "ranges": [{"talk_time": 40}]},
                        {"type": "unknown", "ranges": [{"talk_time": 20}]},
                    ]
                }
            }
        }
    }
    assert talk_seconds(body) == (40.0, 100.0)


def test_published_percent_is_used_when_clusters_have_no_talk_time() -> None:
    body = {"data": {"attributes": {"metrics": [{"name": "rep_talk_percent", "value": 48}]}}}
    assert talk_seconds(body) == (48.0, 100.0)
    assert talk_seconds({"data": {"attributes": {}}}) is None


@pytest.mark.parametrize(
    ("value", "points"),
    [(None, None), (0, 2), (50, 2), (50.0, 2), (50.1, 0), (100, 0)],
)
def test_points_are_full_at_or_under_the_target(value, points) -> None:
    assert call_talk_ratio_points(value) == points


def test_month_share_weights_longer_calls() -> None:
    readings = _score(
        [
            _call("short", when="2026-09-02"),
            _call("long", when="2026-09-18"),
            _call("other", account=OTHER, when="2026-09-04"),
            _call("email", kind="email", when="2026-09-04"),
            _call("august", when="2026-08-20"),
        ],
        {
            "short": (40.0, 100.0),
            "long": (10.0, 10.0),
            "other": (10.0, 100.0),
            "august": (90.0, 100.0),
        },
    )
    shared = readings[SITE_A]
    assert shared["value"] == round(100.0 * 50 / 110, 4)
    assert shared["points"] == 2
    assert shared["period_key"] == "2026-09"
    assert shared["meta"]["window_start"] == "2026-08-30"
    assert shared["meta"]["calls"] == 2
    assert shared["meta"]["scored_calls"] == 2
    assert readings[SITE_B]["value"] == shared["value"]
    assert readings[OTHER]["value"] == 10
    assert readings[OTHER]["points"] == 2


def test_missing_split_stays_unscored_and_no_calls_stay_unscored() -> None:
    readings = _score([_call("bare", account=OTHER)], {"bare": {"data": {"attributes": {}}}})
    assert readings[OTHER]["points"] is None
    assert readings[OTHER]["value"] is None
    assert "talk-time split" in readings[OTHER]["error"]
    assert readings[SITE_A]["points"] is None
    assert readings[SITE_A]["value"] is None
    assert readings[SITE_A]["error"] is None
    assert readings[SITE_A]["meta"]["calls"] == 0


def test_over_fifty_scores_zero() -> None:
    readings = _score([_call("loud", account=OTHER)], {"loud": (51.0, 100.0)})
    assert readings[OTHER]["value"] == 51
    assert readings[OTHER]["points"] == 0


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
        metric_name="call_talk_ratio",
        grain="monthly",
        as_of="2026-09-30",
        period_key="2026-09",
        value=40,
        points=2,
        generator="manual-seed",
    )
    conn.close()
    result = backfill_call_talk_ratio(
        months=2,
        entities=ENTITIES,
        as_of=AS_OF,
        engagements=[
            _call("aug", when="2026-08-12"),
            _call("sep", when="2026-09-12"),
        ],
        shares={"aug": (20.0, 100.0), "sep": (80.0, 100.0)},
        parent_ids=PARENTS,
        persist=True,
    )
    assert result["months_skipped"] == ["2026-09"]
    assert [row["period_key"] for row in result["months"]] == ["2026-08"]
    assert result["months"][0]["scored"] == 2
    conn = connect()
    try:
        august = observations_for_metric(conn, SITE_A, "call_talk_ratio")
        september = observations_for_metric(conn, SITE_A, "call_talk_ratio")
    finally:
        conn.close()
    assert any(row["period_key"] == "2026-08" and row["points"] == 2 for row in august)
    assert any(row["period_key"] == "2026-09" and row["generator"] == "manual-seed" for row in september)


def test_points_reject_a_value_outside_the_scale() -> None:
    with pytest.raises(CallTalkRatioGeneratorError, match="0 to 100"):
        call_talk_ratio_points(101)


def test_negative_talk_time_is_rejected() -> None:
    body = {
        "data": {
            "attributes": {
                "recording": {"clusters": [{"type": "rep", "ranges": [{"talk_time": -1}]}]}
            }
        }
    }
    with pytest.raises(CallTalkRatioGeneratorError, match="negative"):
        talk_seconds(body)
