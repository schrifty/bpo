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


def test_open_age_days_and_backlog_buckets_logic():
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    recent = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    assert JiraClient._open_age_days({"created": old}) > 30
    assert JiraClient._open_age_days({"created": recent}) <= 7
