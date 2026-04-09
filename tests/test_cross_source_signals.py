"""Unit tests for cross-source Notable Signals enrichment."""

from __future__ import annotations

import datetime

import pytest

from src.cross_source_signals import extend_health_report_signals


def test_extend_appends_jira_escalated_and_respects_cap():
    base = ["Pendo baseline signal"]
    report = {
        "signals": list(base),
        "jira": {
            "escalated": 2,
            "open_bugs": 1,
            "open_issues": 3,
            "resolved_issues": 10,
            "total_issues": 20,
            "days": 90,
        },
        "csr": {"platform_health": {"error": "none"}},
        "salesforce": {},
        "champions": [{"x": 1}],
        "at_risk_users": [],
        "account": {"total_visitors": 5},
    }
    extend_health_report_signals(report)
    sigs = report["signals"]
    assert sigs[0] == "Pendo baseline signal"
    assert any("escalated" in s for s in sigs)
    assert len(sigs) <= 22


def test_extend_salesforce_pipeline_and_renewal():
    end = (datetime.date.today() + datetime.timedelta(days=60)).strftime("%Y-%m-%d")
    report = {
        "signals": [],
        "jira": {},
        "salesforce": {
            "matched": True,
            "accounts": [
                {
                    "Name": "Acme Corp",
                    "ARR__c": 50000,
                    "Contract_Contract_End_Date__c": end,
                }
            ],
            "pipeline_arr": 120000.0,
            "opportunity_count_this_year": 1,
        },
        "champions": [],
        "at_risk_users": [],
        "account": {"total_visitors": 20},
    }
    extend_health_report_signals(report)
    texts = " ".join(report["signals"])
    assert "pipeline" in texts.lower() or "Commercial" in texts
    assert "renewal" in texts.lower() or "contract end" in texts.lower()


def test_extend_cs_red_health():
    report = {
        "signals": [],
        "jira": {},
        "csr": {
            "platform_health": {
                "health_distribution": {"RED": 2, "YELLOW": 0, "GREEN": 0},
                "total_critical_shortages": 5,
                "total_shortages": 100,
                "factory_count": 3,
                "sites": [],
            },
        },
        "champions": [],
        "at_risk_users": [],
        "account": {"total_visitors": 10},
    }
    extend_health_report_signals(report)
    joined = " ".join(report["signals"]).lower()
    assert "red" in joined
    assert "critical" in joined or "shortage" in joined


def test_extend_people_at_risk():
    report = {
        "signals": [],
        "jira": {},
        "champions": [],
        "at_risk_users": [{"u": i} for i in range(5)],
        "account": {"total_visitors": 30},
    }
    extend_health_report_signals(report)
    assert any("at-risk" in s.lower() for s in report["signals"])


def test_extend_skips_redundant_duplicate():
    report = {
        "signals": ["Support: 2 escalated or engineering-queue ticket(s) (Jira HELP, 90d)"],
        "jira": {"escalated": 2, "days": 90},
        "account": {"total_visitors": 5},
    }
    extend_health_report_signals(report)
    assert report["signals"].count(
        "Support: 2 escalated or engineering-queue ticket(s) (Jira HELP, 90d)"
    ) == 1


def test_extend_no_jira_error_branch():
    report = {
        "signals": ["base"],
        "jira": {"error": "offline"},
        "account": {"total_visitors": 5},
    }
    extend_health_report_signals(report)
    assert report["signals"] == ["base"]
