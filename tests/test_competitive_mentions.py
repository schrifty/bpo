"""Competitive-mention flag from Chorus transcripts."""

from datetime import date

from src.healthscore_web.competitive_mentions import (
    CompetitiveMentionsGeneratorError,
    get_competitive_mentions,
    mentioned_competitors,
    transcript_text,
)
from src.healthscore_web.framework import load_framework

import pytest

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


def _call(engagement_id: str, *, account: str = PARENT, when: str = "2026-09-10") -> dict:
    return {
        "engagement_id": engagement_id,
        "account_id": account,
        "engagement_type": "meeting",
        "no_show": False,
        "processing_state": "done",
        "duration": 600,
        "date_time": when,
    }


def _conversation(snippet: str) -> dict:
    return {"data": {"attributes": {"recording": {"utterances": [{"snippet": snippet}]}, "summary": "Kinaxis"}}}


def _score(engagements: list[dict], transcripts: dict[str, str]) -> dict:
    result = get_competitive_mentions(
        entities=ENTITIES,
        engagements=engagements,
        transcripts=transcripts,
        parent_ids=PARENTS,
        as_of=AS_OF,
        persist=False,
    )
    return {row["entity_id"]: row for row in result["readings"]}


def test_framework_competitive_mentions_uses_chorus_only() -> None:
    component = next(row for row in load_framework()["overrides"] if row["key"] == "competitive_mentions")
    assert component["data_source"] == ["Chorus"]
    assert component["metric-generator"] == "get_competitive_mentions"
    assert component["status"] == "defined"
    assert "Gmail" not in component["description"]


def test_matcher_finds_listed_competitors_and_skips_ordinary_words() -> None:
    assert mentioned_competitors("They are evaluating Kinaxis and o9 Solutions.") == [
        "Kinaxis",
        "o9 Solutions",
    ]
    assert "SAP Integrated Business Planning (SAP IBP)" in mentioned_competitors("SAP IBP came up.")
    assert "SAP S/4HANA" in mentioned_competitors("The ERP is S/4HANA.")
    assert "Oracle ERP/PeopleSoft" in mentioned_competitors("PeopleSoft is still live.")
    assert "GAINS" in mentioned_competitors("We looked at GAINS last year.")
    assert "Board" in mentioned_competitors("Board is on the shortlist.")
    assert mentioned_competitors("Inventory gains and a board meeting about SAP and Oracle.") == []
    assert mentioned_competitors("More information on the dashboard.") == []
    assert mentioned_competitors("Baxter the customer site reviewed its own plan.") == []
    assert "Baxter Planning" in mentioned_competitors("Baxter Planning sent a proposal.")
    assert mentioned_competitors("Plexiglas covers the line.") == []
    assert "Plex Systems" in mentioned_competitors("They demoed Plex Systems.")
    assert mentioned_competitors(None) == []


def test_transcript_uses_utterances_and_ignores_the_summary() -> None:
    assert transcript_text(_conversation("We use Blue Yonder.")) == "We use Blue Yonder."
    assert mentioned_competitors(transcript_text(_conversation("No vendor named."))) == []


def test_parent_call_raises_the_flag_for_child_entities() -> None:
    readings = _score(
        [_call("eng-1"), _call("eng-2", account=OTHER, when="2026-09-12")],
        {"eng-1": "Switching to Kinaxis next quarter.", "eng-2": "Nothing about vendors."},
    )
    assert readings[SITE_A]["value"] == 1
    assert readings[SITE_B]["value"] == 1
    assert readings[SITE_A]["points"] is None
    assert readings[SITE_A]["meta"]["competitors"] == ["Kinaxis"]
    assert readings[OTHER]["value"] == 0
    assert readings[OTHER]["meta"]["calls"] == 1


def test_no_calls_and_calls_outside_the_month_leave_the_flag_off() -> None:
    readings = _score([_call("eng-old", when="2026-08-02")], {"eng-old": "Kinaxis"})
    assert [row["value"] for row in readings.values()] == [0, 0, 0]
    with pytest.raises(CompetitiveMentionsGeneratorError):
        get_competitive_mentions(entities=[], engagements=[], transcripts={}, as_of=AS_OF, persist=False)
