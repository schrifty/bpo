"""Active engineering KPI deck: selection, notable moves, slide requests."""

from __future__ import annotations

from src.eng_kpi_deck import (
    ERROR_DETAIL_MAX,
    EngKpiCard,
    HistoryPoint,
    append_kpi_slide,
    append_notable_slide,
    append_title_slide,
    build_engineering_kpi_slide_requests,
    classify_notable_change,
    error_callout,
    is_active_engineering_kpi,
    iter_active_engineering_kpis,
    merge_live_history,
)
from src.metrics_digest import DigestRow
from src.metrics_registry import load_metrics_registry


def _row(
    name: str,
    value: float,
    *,
    target: float | None = None,
    direction: str | None = "lower",
    unit: str | None = None,
) -> DigestRow:
    off = False
    if target is not None and direction == "lower":
        off = value > target
    if target is not None and direction == "higher":
        off = value < target
    return DigestRow(
        name=name,
        metric_id=None,
        value=value,
        target=target,
        direction=direction,
        off_target=off,
        unit=unit,
    )


def test_active_engineering_kpis_skip_pending_and_support() -> None:
    names = {name for name, _ in iter_active_engineering_kpis()}
    assert "Customer-Reported Bugs" in names
    assert "PRs Merged" in names
    assert "SDD Adoption %" not in names
    assert "Engineering Escalation Rate" not in names
    registry = load_metrics_registry()
    for name, entry in iter_active_engineering_kpis(registry=registry):
        assert is_active_engineering_kpi(entry), name
        tags = set(entry.get("tags") or [])
        assert "pending" not in {str(t).lower() for t in tags}


def test_classify_notable_improvement_for_lower_is_better() -> None:
    change = classify_notable_change(
        previous=20.0,
        current=12.0,
        direction="lower",
        target=15.0,
        unit=None,
        name="Customer-Reported Bugs",
        value_display="12",
        previous_display="20",
    )
    assert change is not None
    assert change.kind == "success"
    assert "12" in change.summary


def test_classify_notable_regression_for_higher_is_better() -> None:
    change = classify_notable_change(
        previous=40.0,
        current=28.0,
        direction="higher",
        target=50.0,
        unit=None,
        name="PRs Merged",
        value_display="28",
        previous_display="40",
    )
    assert change is not None
    assert change.kind == "failure"


def test_classify_ignores_small_moves() -> None:
    change = classify_notable_change(
        previous=100.0,
        current=104.0,
        direction="higher",
        target=None,
        unit=None,
        name="Tokens",
        value_display="104",
        previous_display="100",
    )
    assert change is None


def test_classify_percent_points_and_target_cross() -> None:
    change = classify_notable_change(
        previous=18.0,
        current=14.0,
        direction="lower",
        target=15.0,
        unit="percent",
        name="Defects / 100",
        value_display="14%",
        previous_display="18%",
    )
    assert change is not None
    assert change.kind == "success"
    assert "on target" in change.summary


def test_merge_live_history_replaces_same_period() -> None:
    hist = [HistoryPoint("2026-07", 10.0), HistoryPoint("2026-08", 11.0)]
    merged = merge_live_history(hist, live_value=12.0, period_key="2026-08")
    assert merged[-1] == HistoryPoint("2026-08", 12.0)
    assert len(merged) == 2


def test_merge_live_history_appends_new_period() -> None:
    hist = [HistoryPoint("2026-07", 10.0)]
    merged = merge_live_history(hist, live_value=12.0, period_key="2026-08")
    assert [p.period_key for p in merged] == ["2026-07", "2026-08"]


def _card(
    *,
    name: str = "PRs Merged",
    value: float = 40.0,
    history: tuple[HistoryPoint, ...] = (),
    notable=None,
) -> EngKpiCard:
    return EngKpiCard(
        row=_row(name, value, target=50.0, direction="higher"),
        grain="month",
        history=history,
        notable=notable,
    )


class _FakeCharts:
    def add_line_chart(self, **kwargs: object) -> tuple[str, int]:
        self.kwargs = kwargs
        return "ss-1", 9


def _all_text(reqs: list) -> str:
    return " ".join(
        r["insertText"]["text"]
        for r in reqs
        if isinstance(r, dict) and "insertText" in r
    )


def test_title_and_notable_slides_are_factual() -> None:
    reqs: list = []
    cards = [_card()]
    append_title_slide(reqs, cards, as_of="2026-09-14", slide_index=0)
    body = _all_text(reqs)
    assert "Engineering KPIs" in body
    assert "No overall score" in body
    assert "as of 2026-09-14" in body

    reqs2: list = []
    append_notable_slide(reqs2, cards, slide_index=1)
    empty = _all_text(reqs2)
    assert "No period-to-period move" in empty


def test_kpi_slide_embeds_chart_when_history_exists() -> None:
    reqs: list = []
    hist = (HistoryPoint("2026-06", 30.0), HistoryPoint("2026-07", 36.0), HistoryPoint("2026-08", 40.0))
    charts = _FakeCharts()
    append_kpi_slide(reqs, _card(history=hist), slide_index=2, charts=charts)
    body = _all_text(reqs)
    assert "PRs Merged" in body
    assert "Current" in body
    assert any(
        isinstance(r, dict) and "createSheetsChart" in r for r in reqs
    )
    assert charts.kwargs["series"]["Value"] == [30.0, 36.0, 40.0]


def _error_card(name: str = "AI Token Usage", error: str = "HTTPError: 401 Unauthorized") -> EngKpiCard:
    row = DigestRow(
        name=name,
        metric_id=None,
        value=None,
        target=None,
        direction="higher",
        off_target=True,
        error=error,
    )
    return EngKpiCard(row=row, grain="month", history=(), notable=None)


def test_error_callout_trims_long_detail() -> None:
    line = error_callout("boom " * 80)
    assert line.startswith("Could not compute: boom")
    assert len(line) <= len("Could not compute: ") + ERROR_DETAIL_MAX


def test_kpi_slide_names_the_failing_source() -> None:
    reqs: list = []
    append_kpi_slide(reqs, _error_card(), slide_index=2, charts=None)
    body = _all_text(reqs)
    assert "Could not compute: HTTPError: 401 Unauthorized" in body


def test_title_slide_counts_and_names_failed_kpis() -> None:
    reqs: list = []
    append_title_slide(reqs, [_card(), _error_card()], as_of="2026-09-20", slide_index=0)
    body = _all_text(reqs)
    assert "1 KPI(s) could not be computed" in body
    assert "AI Token Usage" in body


def test_build_plan_orders_cover_then_notables_then_kpis() -> None:
    cards = [_card(name="AI Token Usage"), _card(name="PRs Merged")]
    reqs, sids = build_engineering_kpi_slide_requests(cards, as_of="2026-09-14", charts=None)
    assert sids[0] == "eng_kpis_title"
    assert sids[1] == "eng_kpis_notable"
    assert len(sids) == 4
    assert reqs
