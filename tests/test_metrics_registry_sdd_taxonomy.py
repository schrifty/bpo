"""Legacy / forward / pending taxonomy for the SDD transition.

These rules exist so the Spec-Driven Dev adoption curve stays measurable: every
engineering KPI declares whether it survives full SDD adoption, and rows that
cannot be instrumented yet are explicit rather than silently broken.
"""

from __future__ import annotations

import pytest

from src.metrics_registry import (
    has_metric_generator,
    iter_all_metrics,
    load_metrics_registry,
    registry_metric_mgmt_guidance,
    registry_metric_tags,
    registry_metric_target,
)

# Rows added from the SDD-era scorecard ahead of the Gemba go-live.
FORWARD_SDD_METRICS = (
    "SDD Adoption %",
    "Spec-to-Production Cycle Time",
    "Escaped Defect Rate",
    "QA Cycle Time",
    "SLO Attainment %",
    "MTTR (Production Incidents)",
    "Engineering + Infrastructure Cost % of ARR",
    "Idea-to-Value Cycle Time",
)

# Must exist before the October go-live: Jira cannot reconstruct a status that
# was not being written at the time, so a late start means no pre-SDD baseline.
GO_LIVE_BLOCKING = ("SDD Adoption %", "Spec-to-Production Cycle Time")


@pytest.fixture(scope="module")
def metrics() -> dict:
    return load_metrics_registry()["metrics"]


def test_engineering_rows_declare_exactly_one_era(metrics: dict) -> None:
    """Every engineering KPI is either legacy or forward, never both or neither.

    Support rows are exempt: there ``engineering`` names the escalation target,
    not the owning function.
    """
    for name, entry in iter_all_metrics(registry={"metrics": metrics}):
        tags = set(registry_metric_tags(entry))
        if "engineering" not in tags or "support" in tags:
            continue
        era = tags & {"legacy", "forward"}
        assert len(era) == 1, f"{name} must carry exactly one of legacy/forward, got {sorted(era)}"


def test_pending_rows_have_no_generator_and_no_target(metrics: dict) -> None:
    """A pending KPI has no value, not a bad value — it must never score as a miss."""
    for name, entry in iter_all_metrics(registry={"metrics": metrics}):
        if "pending" not in registry_metric_tags(entry):
            continue
        assert not has_metric_generator(entry), f"{name} is pending but has a generator"
        assert registry_metric_target(entry) is None, f"{name} is pending but has a target"
        assert entry.get("direction") in (None, ""), f"{name} is pending but has a direction"


def test_pending_rows_are_forward_and_record_their_unblocker(metrics: dict) -> None:
    for name, entry in iter_all_metrics(registry={"metrics": metrics}):
        tags = registry_metric_tags(entry)
        if "pending" not in tags:
            continue
        assert "forward" in tags, f"{name} is pending but not forward"
        assert "PENDING —" in (entry.get("description") or ""), (
            f"{name} must state what unblocks it in its description"
        )


def test_forward_sdd_metrics_are_registered(metrics: dict) -> None:
    for name in FORWARD_SDD_METRICS:
        entry = metrics[name]
        tags = registry_metric_tags(entry)
        assert "forward" in tags and "pending" in tags
        assert registry_metric_mgmt_guidance(entry)


def test_sdd_tag_marks_exactly_the_go_live_blocking_rows(metrics: dict) -> None:
    """``sdd`` means "measures SDD itself", so it is the go-live instrumentation list.

    Kept exact: the infographic badges these rows as deadline-bound, and a stray
    tag would contradict the count printed beside them.
    """
    tagged = {
        name
        for name, entry in iter_all_metrics(registry={"metrics": metrics})
        if "sdd" in registry_metric_tags(entry)
    }
    assert tagged == set(GO_LIVE_BLOCKING)


def test_superseded_legacy_rows_stay_tagged_legacy(metrics: dict) -> None:
    """Rows a forward KPI replaces keep reporting until SDD is adopted, then sunset."""
    for name in (
        "Issues Shipped",
        "Defects per 100 Issues",
        "Headcount + AI Spend / Issue",
        "Customer-Reported Bugs",
        "Site Uptime %",
    ):
        assert "legacy" in registry_metric_tags(metrics[name])


def test_ai_adoption_rows_are_forward(metrics: dict) -> None:
    """Tool adoption outlives the ticket-based delivery model, so it is not legacy."""
    for name in ("AI Code Share", "Weekly Active AI Users", "Tokens per Dev"):
        assert "forward" in registry_metric_tags(metrics[name])
