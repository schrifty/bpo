"""Tests for the Cursor usage report aggregator and deck enrichment filter."""

from __future__ import annotations

from datetime import datetime, timezone

from src.cursor_usage_report import (
    build_cursor_usage_report,
    generate_cursor_usage_takeaway,
)
from src.deck_data_enrichment import enrich_cursor_usage_if_needed


def _ms(y: int, m: int, d: int = 1) -> int:
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000)


class _FakeClient:
    def get_team_members(self, **k):
        return [{"email": f"u{i}@x.com"} for i in range(10)]

    def get_daily_usage(self, start, end, **k):
        return [
            {"date": _ms(2026, 3, 1), "userId": 1, "isActive": True, "agentRequests": 5, "chatRequests": 3},
            {"date": _ms(2026, 3, 2), "userId": 2, "isActive": True, "composerRequests": 4},
            {"date": _ms(2026, 4, 1), "userId": 1, "isActive": True, "agentRequests": 2},
        ]

    def get_usage_events(self, start, end, **k):
        return [
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u1@x.com", "model": "claude-4.5-sonnet",
             "tokenUsage": {"inputTokens": 100, "outputTokens": 50}},
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u2@x.com", "model": "gpt-5",
             "tokenUsage": {"inputTokens": 200, "outputTokens": 25}},
            {"timestamp": str(_ms(2026, 4, 2)), "userEmail": "u1@x.com", "model": "claude-4.5-sonnet",
             "tokenUsage": {"inputTokens": 300, "outputTokens": 100}},
            {"timestamp": str(_ms(2026, 4, 2)), "userEmail": "u3@x.com", "model": "gpt-5",
             "tokenUsage": {"inputTokens": 10, "outputTokens": 5}},
        ]

    def get_spend(self, **k):
        return [
            {"email": "u1@x.com", "overallSpendCents": 4200},
            {"email": "u2@x.com", "overallSpendCents": 3100},
            {"email": "u3@x.com", "overallSpendCents": 100},
        ]


def test_build_report_aggregates_all_sections() -> None:
    rep = build_cursor_usage_report(client=_FakeClient())
    assert rep["configured"] is True
    assert rep["members"]["total"] == 10
    # u1 (450+550=... ) tokens across two events, u2 225 → totals 550+225=... check sum
    assert rep["totals"]["total_tokens"] == 100 + 50 + 200 + 25 + 300 + 100 + 10 + 5
    assert rep["totals"]["spend_cents_cycle"] == 7400.0
    # Two months present, ordered.
    assert [m["label"] for m in rep["monthly"]] == ["Mar", "Apr"]
    # Top user is u1 (most tokens), carries spend.
    assert rep["top_users"][0]["email"] == "u1@x.com"
    assert rep["top_users"][0]["spend_cents"] == 4200
    assert all(
        u["email"] not in {t["email"] for t in rep["top_users"]}
        for u in rep["bottom_users"]
    )
    # Model mix sorted by tokens desc; shares sum ~1.
    assert rep["model_mix"][0]["model"] == "claude-4.5-sonnet"
    assert abs(sum(m["share"] for m in rep["model_mix"]) - 1.0) < 1e-6
    assert rep["errors"] == []


class _RichClient(_FakeClient):
    """Client whose events carry cost (chargedCents) and span multiple days/models."""

    def get_usage_events(self, start, end, **k):
        return [
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u1@x.com", "model": "claude-4.5-sonnet",
             "tokenUsage": {"inputTokens": 100, "outputTokens": 50}, "chargedCents": 12.0},
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u2@x.com", "model": "gpt-5",
             "tokenUsage": {"inputTokens": 200, "outputTokens": 25}, "chargedCents": 8.0},
            {"timestamp": str(_ms(2026, 4, 2)), "userEmail": "u1@x.com", "model": "claude-4.5-sonnet",
             "tokenUsage": {"inputTokens": 300, "outputTokens": 100}, "chargedCents": 30.0},
            {"timestamp": str(_ms(2026, 4, 2)), "userEmail": "u1@x.com", "model": "gpt-5",
             "tokenUsage": {"inputTokens": 50, "outputTokens": 10}, "chargedCents": 5.0},
        ]


def test_bottom_users_are_lowest_and_exclude_top() -> None:
    class _WideClient(_FakeClient):
        def get_usage_events(self, start, end, **k):
            rows = []
            for i, toks in enumerate((9000, 8000, 7000, 6000, 5000, 4000, 3000, 2000, 1000, 500), start=1):
                rows.append(
                    {"timestamp": str(_ms(2026, 4, 1)), "userEmail": f"u{i}@x.com", "model": "gpt-5",
                     "tokenUsage": {"inputTokens": toks, "outputTokens": 0}}
                )
            return rows

        def get_spend(self, **k):
            return [{"email": f"u{i}@x.com", "overallSpendCents": i * 100} for i in range(1, 11)]

    rep = build_cursor_usage_report(client=_WideClient())
    top = rep["top_users"]
    bottom = rep["bottom_users"]
    assert len(top) == 6
    assert len(bottom) == 4
    assert bottom[0]["tokens"] <= bottom[-1]["tokens"] <= top[-1]["tokens"]
    assert {u["email"] for u in bottom}.isdisjoint({u["email"] for u in top})


def test_build_report_includes_cost_daily_and_matrix() -> None:
    rep = build_cursor_usage_report(client=_RichClient())

    # Window cost = sum of chargedCents across events.
    assert rep["totals"]["charged_cents_window"] == 12.0 + 8.0 + 30.0 + 5.0

    # Daily series: two days, chronological, with cost + active users + io tokens.
    daily = rep["daily"]
    assert [d["date"] for d in daily] == ["2026-04-01", "2026-04-02"]
    assert daily[0]["cents"] == 20.0 and daily[0]["active_users"] == 2
    assert daily[1]["cents"] == 35.0 and daily[1]["active_users"] == 1
    assert daily[0]["input_tokens"] == 300 and daily[0]["output_tokens"] == 75

    # Model mix carries per-model cost.
    sonnet = next(m for m in rep["model_mix"] if m["model"] == "claude-4.5-sonnet")
    assert sonnet["cents"] == 42.0

    # Top user (u1) carries window cost, io split, and a model breakdown.
    u1 = rep["top_users"][0]
    assert u1["email"] == "u1@x.com"
    assert u1["window_cents"] == 47.0
    assert u1["input_tokens"] == 450 and u1["output_tokens"] == 160
    assert u1["models"] and u1["models"][0]["model"] == "claude-4.5-sonnet"

    # Model-usage-by-user matrix: users x models, series aligned to users order.
    matrix = rep["user_model_matrix"]
    assert "u1@x.com" in matrix["users"]
    assert set(matrix["models"]).issubset({"claude-4.5-sonnet", "gpt-5", "Other"})
    for series in matrix["series"].values():
        assert len(series) == len(matrix["users"])


class _EffClient(_FakeClient):
    """Client with per-user accepted lines (daily-usage) and tokens above the floor."""

    def get_daily_usage(self, start, end, **k):
        return [
            {"date": _ms(2026, 4, 1), "userId": 1, "email": "u1@x.com", "isActive": True,
             "agentRequests": 5, "acceptedLinesAdded": 180, "totalLinesAdded": 200,
             "totalAccepts": 40, "totalRejects": 4},
            {"date": _ms(2026, 4, 2), "userId": 1, "email": "u1@x.com", "isActive": True,
             "acceptedLinesAdded": 120, "totalLinesAdded": 150},
            {"date": _ms(2026, 4, 1), "userId": 2, "email": "u2@x.com", "isActive": True,
             "acceptedLinesAdded": 60, "totalLinesAdded": 100},
        ]

    def get_usage_events(self, start, end, **k):
        return [
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u1@x.com", "model": "claude-4.5-sonnet",
             "tokenUsage": {"inputTokens": 4000, "outputTokens": 1000}, "chargedCents": 50.0},
            {"timestamp": str(_ms(2026, 4, 2)), "userEmail": "u1@x.com", "model": "claude-4.5-sonnet",
             "tokenUsage": {"inputTokens": 3000, "outputTokens": 1000}, "chargedCents": 40.0},
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u2@x.com", "model": "gpt-5",
             "tokenUsage": {"inputTokens": 1500, "outputTokens": 500}, "chargedCents": 20.0},
        ]


def test_efficiency_block_joins_lines_and_tokens() -> None:
    rep = build_cursor_usage_report(client=_EffClient())
    eff = rep["efficiency"]
    # Team totals: accepted 300+60=360, total 350+100=450 -> 80% kept.
    assert eff["accepted_lines"] == 360
    assert eff["total_lines"] == 450
    assert eff["lines_kept"] == 0.8
    # Tokens 9000 (u1) + 2000 (u2) = 11000; cost 90 + 20 = 110.
    assert eff["total_tokens"] == 11000
    assert eff["cost_per_accepted_line_cents"] == round(110 / 360, 4)
    assert eff["accepted_lines_per_1k_tokens"] == round(360 / 11, 2)
    # Per-engineer ranking by lines per 1K tokens: u1 (33.33) before u2 (30).
    ranked = eff["top_efficiency"]
    assert [u["email"] for u in ranked] == ["u1@x.com", "u2@x.com"]
    assert ranked[0]["lines_per_1k_tokens"] == round(300 / 9, 2)
    # Daily series merges accepted lines with per-day cost.
    assert any(d.get("cents") for d in eff["daily"])
    assert any(d.get("accepted_lines") for d in eff["daily"])


class _EffJoinMissClient(_EffClient):
    """Adds an engineer with token usage but no accepted-line row (join miss)."""

    def get_usage_events(self, start, end, **k):
        base = _EffClient.get_usage_events(self, start, end, **k)
        base.append(
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u3@x.com", "model": "gpt-5",
             "tokenUsage": {"inputTokens": 2000, "outputTokens": 500}, "chargedCents": 15.0}
        )
        return base


def test_efficiency_warns_on_token_only_engineer() -> None:
    rep = build_cursor_usage_report(client=_EffJoinMissClient())
    assert any("no accepted-line data" in w for w in rep["warnings"])
    # u3 has tokens but no accepted lines -> excluded from the efficiency ranking.
    assert "u3@x.com" not in [u["email"] for u in rep["efficiency"]["top_efficiency"]]


def test_focus_takeaways_return_per_slide_keys(monkeypatch) -> None:
    from src.cursor_usage_report import generate_cursor_usage_takeaways

    class _Msg:
        content = "Concrete sentence with a number 12 and an action: reclaim 3 idle seats."

    class _Choice:
        message = _Msg()

    class _Resp:
        choices = [_Choice()]

    class _Completions:
        def create(self, **k):
            return _Resp()

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    monkeypatch.setattr("src.config.llm_client", lambda: _Client())
    rep = build_cursor_usage_report(client=_RichClient())
    out = generate_cursor_usage_takeaways(rep)
    assert set(out.keys()) == {"cost", "usage", "users", "efficiency"}
    assert all(v for v in out.values())


def test_build_report_section_failure_is_collected() -> None:
    from src.cursor_client import CursorClientError

    class _SpendBoom(_FakeClient):
        def get_spend(self, **k):
            raise CursorClientError("spend down")

    rep = build_cursor_usage_report(client=_SpendBoom())
    assert rep["configured"] is True
    assert any("spend" in e for e in rep["errors"])
    # Other sections still populated.
    assert rep["totals"]["total_tokens"] > 0
    assert rep["totals"]["spend_cents_cycle"] is None


class _RemovedUserClient(_FakeClient):
    """Events where some rows lack userEmail (since-removed accounts)."""

    def get_usage_events(self, start, end, **k):
        return [
            {"timestamp": str(_ms(2026, 4, 1)), "userEmail": "u1@x.com", "model": "gpt-5",
             "tokenUsage": {"inputTokens": 100, "outputTokens": 50}},
            # Removed user: Cursor drops userEmail but the tokens still count in aggregate.
            {"timestamp": str(_ms(2026, 4, 1)), "model": "gpt-5",
             "tokenUsage": {"inputTokens": 400, "outputTokens": 0}},
        ]


def test_unattributed_events_emit_warning_but_keep_totals() -> None:
    rep = build_cursor_usage_report(client=_RemovedUserClient())
    # Aggregate token total includes the unattributed (removed-user) event.
    assert rep["totals"]["total_tokens"] == 100 + 50 + 400
    # A warning is surfaced (not an error) about the unattributable slice.
    assert rep["errors"] == []
    assert any("no userEmail" in w for w in rep["warnings"])
    # Only the attributed user shows up in top_users.
    assert [u["email"] for u in rep["top_users"]] == ["u1@x.com"]


def test_no_warning_when_all_events_attributed() -> None:
    rep = build_cursor_usage_report(client=_FakeClient())
    assert all("no userEmail" not in w for w in rep["warnings"])


def test_enrich_drops_slide_when_unconfigured(monkeypatch) -> None:
    monkeypatch.setattr("src.cursor_client.cursor_configured", lambda: False)
    plan = [{"slide_type": "eng_velocity"}, {"slide_type": "cursor_usage"}, {"slide_type": "data_quality"}]
    out = enrich_cursor_usage_if_needed({}, plan)
    assert [e["slide_type"] for e in out] == ["eng_velocity", "data_quality"]


def test_enrich_noop_when_no_cursor_slide(monkeypatch) -> None:
    # Should not even check config when the plan has no cursor slide.
    plan = [{"slide_type": "eng_velocity"}]
    out = enrich_cursor_usage_if_needed({}, plan)
    assert out == plan


def test_takeaway_returns_empty_when_unconfigured() -> None:
    assert generate_cursor_usage_takeaway({"configured": False}) == ""
    assert generate_cursor_usage_takeaway({}) == ""


def test_takeaway_returns_empty_on_llm_failure(monkeypatch) -> None:
    def _boom():
        raise RuntimeError("no api key")

    monkeypatch.setattr("src.config.llm_client", _boom)
    rep = build_cursor_usage_report(client=_FakeClient())
    # Even with a valid report, an LLM failure yields no band (no placeholder text).
    assert generate_cursor_usage_takeaway(rep) == ""


def test_takeaway_uses_llm_sentence(monkeypatch) -> None:
    class _Msg:
        content = "- Adoption is at 70% with $73 spend; reclaim 8 idle seats before the next renewal."

    class _Choice:
        message = _Msg()

    class _Resp:
        choices = [_Choice()]

    class _Completions:
        def create(self, **k):
            return _Resp()

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    monkeypatch.setattr("src.config.llm_client", lambda: _Client())
    rep = build_cursor_usage_report(client=_FakeClient())
    out = generate_cursor_usage_takeaway(rep)
    assert out.startswith("Adoption is at 70%")
    assert "- " not in out[:2]  # leading bullet stripped
