"""Claude site analysis: digest facts and the report endpoint."""

from pathlib import Path

import pytest

from src.healthscore_web.analysis import (
    HealthscoreAnalysisError,
    _prompt,
    build_analysis_digest,
    generate_site_analysis,
)
from src.healthscore_web.framework import load_framework
from tests.test_healthscore_web import _entities, _score_owner_client
from tests.test_kpi_web_api import _login


def _observation(metric: str, period: str, points: float, value: float | None = None) -> dict:
    return {
        "metric_name": metric,
        "period_key": period,
        "effective_points": points,
        "effective_value": value if value is not None else points,
        "overridden": False,
        "source_mode": "automated",
        "note": None,
    }


def test_digest_lists_scored_inputs_with_history_and_names_gaps() -> None:
    framework = load_framework()
    # Newest period first, as observations_for_entity returns them.
    observations = [
        _observation("usage_level", "2026-W39", 6, 95),
        _observation("meeting_cadence", "2026-08", 2, 2),
        _observation("usage_level", "2026-W38", 3, 55),
    ]
    daily = [
        {"period_key": "2026-09-29", "value": 80.0},
        {"period_key": "2026-09-30", "value": 100.0},
    ]
    digest = build_analysis_digest(
        entity=_entities()[0],
        framework=framework,
        observations=observations,
        daily_scores=daily,
    )
    assert digest["entity"]["name"] == "Acme Entity"
    assert digest["health_score"] == 100
    assert digest["band"] == "green"
    assert digest["scored_inputs"] == 2
    assert digest["overrides_raised"] == []
    assert digest["lead_with"] == []
    usage = next(item for item in digest["inputs"] if item["key"] == "usage_level")
    assert usage["devastating"] is False
    assert usage["latest"]["points"] == 6
    assert usage["latest"]["score_pct"] == 100
    assert [row["period_key"] for row in usage["history"]] == ["2026-W38", "2026-W39"]
    ideas = next(item for item in digest["inputs"] if item["key"] == "product_ideas")
    assert ideas["latest"] is None
    assert ideas["name"] in digest["unscored_inputs"]
    assert digest["daily_scores"][-1] == {"period_key": "2026-09-30", "value": 100.0}
    _, user = _prompt(digest)
    assert user.startswith("Analyze this site. Its band is green.")


def test_very_low_usage_and_roi_are_marked_to_lead_the_analysis() -> None:
    digest = build_analysis_digest(
        entity=_entities()[0],
        framework=load_framework(),
        observations=[
            _observation("usage_level", "2026-W39", 1, 20),
            _observation("roi_multiple", "2026-Q2", 0, 1.4),
            _observation("meeting_cadence", "2026-08", 2, 2),
        ],
        daily_scores=[],
    )
    usage = next(item for item in digest["inputs"] if item["key"] == "usage_level")
    roi = next(item for item in digest["inputs"] if item["key"] == "roi_multiple")
    cadence = next(item for item in digest["inputs"] if item["key"] == "meeting_cadence")
    assert usage["devastating"] is True
    assert roi["devastating"] is True
    assert cadence["devastating"] is False
    assert digest["lead_with"] == [usage["name"], roi["name"]]
    healthy = build_analysis_digest(
        entity=_entities()[0],
        framework=load_framework(),
        observations=[
            _observation("usage_level", "2026-W39", 2, 40),
            _observation("roi_multiple", "2026-Q2", 2, 3.5),
        ],
        daily_scores=[],
    )
    assert healthy["lead_with"] == []


def test_unscored_site_has_no_score_and_empty_analysis_fails_loud() -> None:
    digest = build_analysis_digest(
        entity=_entities()[0],
        framework=load_framework(),
        observations=[],
        daily_scores=[],
    )
    assert digest["health_score"] is None
    assert digest["band"] == "unscored"
    assert digest["scored_inputs"] == 0
    with pytest.raises(HealthscoreAnalysisError):
        generate_site_analysis(digest, invoke=lambda **_: "   ")
    text = generate_site_analysis(digest, invoke=lambda **_: "---\n**Nothing** scored yet.")
    assert text == "Nothing scored yet."


def test_analysis_endpoint_returns_claude_text_for_active_entity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.healthscore_web.api._active_salesforce_entities", _entities)
    seen: dict[str, str] = {}

    def fake_complete(*, system: str, user: str) -> str:
        seen["system"] = system
        seen["user"] = user
        return "Acme is red.\n- Usage level (breadth & depth): 0 of 6.\nCall the champion."

    monkeypatch.setattr("src.healthscore_web.analysis._claude_complete", fake_complete)
    client = _score_owner_client(tmp_path, monkeypatch)
    assert client.get("/healthscore/api/entities/001-active/analysis").status_code == 401
    _login(client)
    saved = client.put(
        "/healthscore/api/entities/001-active/components/usage_level",
        json={"as_of": "2026-09-30", "points": 0},
    )
    assert saved.status_code == 200, saved.text

    missing = client.get("/healthscore/api/entities/001-other/analysis")
    assert missing.status_code == 404

    response = client.get("/healthscore/api/entities/001-active/analysis")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"] is True
    assert body["band"] == "red"
    assert body["health_score"] == 0
    assert body["scored_inputs"] == 1
    assert body["analysis"].startswith("Acme is red.")
    assert "Its band is red." in seen["user"]
    assert "Red: spend most of the analysis on what is wrong" in seen["system"]
    assert "ROI multiple under 3x" in seen["system"]
    assert '"lead_with":["Usage level (breadth & depth)"]' in seen["user"]

    page = client.get("/healthscore/report").text
    assert "Weight scored" not in page
    assert ">Analysis</th>" in page
    assert 'id="hs-analysis-dialog"' in page
