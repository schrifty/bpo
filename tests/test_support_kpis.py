"""Support KPIs deck definition and Jira KPI helpers."""

from src.deck_loader import load_deck, resolve_deck
from src.jira_client import JiraClient


def test_support_kpis_deck_yaml_loads():
    d = load_deck("support-kpis")
    assert d is not None
    assert d.get("id") == "support-kpis"
    assert "KPI" in (d.get("name") or "")


def test_support_kpis_resolves_slide_plan():
    r = resolve_deck("support-kpis", None)
    assert not r.get("error")
    slides = r.get("slides") or []
    types = [s.get("slide_type") or s.get("id") for s in slides]
    assert "support_deck_cover" in types
    assert "support_kpis_intake" in types
    assert "support_kpis_aging_thresholds" in types
    assert "data_quality" in types
    assert types.index("support_kpis_flow") == types.index("support_kpis_intake") + 1


def test_sla_field_adherence_pct():
    issues = [
        {"project": "HELP", "ttfr_ms": 1000, "ttfr_breached": False, "ttfr_waiting": False},
        {"project": "HELP", "ttfr_ms": 2000, "ttfr_breached": True, "ttfr_waiting": False},
        {"project": "HELP", "ttfr_ms": None, "ttfr_waiting": True},
    ]
    out = JiraClient._compute_sla_field_adherence_pct(issues, "ttfr")
    assert out["measured"] == 2
    assert out["met"] == 1
    assert out["pct"] == 50.0


def test_bucket_by_week_resolved_uses_resolutiondate():
    """Flow slide resolved series buckets by resolution date, not last updated."""
    issues = [
        {
            "created": "2026-01-06",
            "updated": "2026-02-01",
            "resolutiondate": "2026-01-20",
            "resolution": "Done",
        },
        {
            "created": "2026-01-08",
            "updated": "2026-01-08",
            "resolutiondate": "",
            "resolution": "",
        },
    ]
    weeks = JiraClient._bucket_by_week(issues)
    assert len(weeks) >= 2
    created_total = sum(w["created"] for w in weeks)
    resolved_total = sum(w["resolved"] for w in weeks)
    assert created_total == 2
    assert resolved_total == 1
    resolved_weeks = [w for w in weeks if w["resolved"] > 0]
    assert resolved_weeks[0]["week"] == "2026-W04"  # 2026-01-20


def test_flow_weekly_in_window_counts_created_and_resolved():
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 3, 1, tzinfo=timezone.utc)
    issues = [
        {
            "created": "2026-02-10",
            "resolutiondate": "2026-02-20",
            "resolution": "Done",
        },
        {
            "created": "2026-02-15",
            "resolutiondate": "",
            "resolution": "",
        },
    ]
    weeks = JiraClient._flow_weekly_in_window(issues, window_days=90, now=now)
    assert sum(w["created"] for w in weeks) == 2
    assert sum(w["resolved"] for w in weeks) == 1


def test_open_age_days_and_backlog_buckets_logic():
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    recent = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    assert JiraClient._open_age_days({"created": old}) > 30
    assert JiraClient._open_age_days({"created": recent}) <= 7
