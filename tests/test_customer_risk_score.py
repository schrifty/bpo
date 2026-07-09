"""Tests for composite §7 customer risk score."""

from __future__ import annotations

from src.customer_risk_score import (
    compute_customer_risk_score,
    portfolio_signals_for_customer,
    signal_severity,
)


def test_signal_severity_counts_keywords():
    assert signal_severity("Read-heavy usage with declining logins") >= 2


def test_acme_scores_higher_than_healthy_beta():
    acme = compute_customer_risk_score(
        pendo={
            "login_pct": 25.0,
            "kei": {"total_queries": 10, "adoption_rate": 10.0},
            "guides": {"dismiss_rate": 5.0},
            "engagement": {"active_rate_7d": 25.0},
        },
        salesforce={
            "commercial_status": "ACTIVE",
            "days_until_contract_end_nearest": 45,
        },
        portfolio_signals=[{"signal": "Low login rate vs peers", "severity": 1}],
        csr_sites=[{"health_score": "YELLOW", "shortages": 3}],
        jira_help={"open_issues": 5, "escalated": 1, "customer_ticket_metrics": {"sla_adherence_1y": {"pct": 70}}},
        include_jira=True,
    )
    beta = compute_customer_risk_score(
        pendo={
            "login_pct": 88.0,
            "kei": {"total_queries": 100, "adoption_rate": 60.0},
            "guides": {"dismiss_rate": 5.0},
            "engagement": {"active_rate_7d": 88.0},
        },
        salesforce={"commercial_status": "ACTIVE", "days_until_contract_end_nearest": 400},
        portfolio_signals=[],
        csr_sites=[],
        jira_help={"open_issues": 0, "escalated": 0},
        include_jira=True,
    )
    assert acme["risk_score"] > beta["risk_score"]
    assert acme["risk_tier"] in ("high", "critical", "medium")
    assert acme["top_influencer"]


def test_contract_churn_maxes_salesforce_pillar():
    churned = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={"commercial_status": "CHURNED"},
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    assert churned["risk_score"] >= 60
    assert churned["risk_tier"] in ("high", "critical")
    assert "salesforce" in churned["top_influencer"].lower()


def test_commercial_status_churned_beats_legacy_active_flag():
    """commercial_status=CHURNED wins even when legacy active fields are absent."""
    churned = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={"commercial_status": "CHURNED", "active": True},
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    assert churned["pillars"]["salesforce"] == 100.0


def test_renewal_in_flight_lowers_salesforce_churn_risk():
    renewal = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={
            "commercial_status": "OUT_OF_CONTRACT_RENEWING",
            "pipeline_arr_including_parent_accounts": 1_455_000.0,
        },
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    churned = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={"commercial_status": "CHURNED"},
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    assert renewal["risk_score"] < churned["risk_score"]
    assert renewal["pillars"]["salesforce"] < churned["pillars"]["salesforce"]


def test_future_commercial_status_not_scored_as_churn():
    future = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={"commercial_status": "FUTURE"},
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    churned = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={"commercial_status": "CHURNED"},
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    assert future["risk_score"] < churned["risk_score"]
    assert future["pillars"]["salesforce"] == 20.0


def test_legacy_renewal_in_flight_fallback_without_commercial_status():
    renewal = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={
            "active": False,
            "renewal_in_flight": True,
            "pipeline_arr_including_parent_accounts": 1_455_000.0,
        },
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    churned = compute_customer_risk_score(
        pendo={"login_pct": 80.0},
        salesforce={"active": False, "renewal_in_flight": False},
        portfolio_signals=[],
        csr_sites=[],
        include_jira=False,
    )
    assert renewal["risk_score"] < churned["risk_score"]
    assert renewal["pillars"]["salesforce"] < churned["pillars"]["salesforce"]


def test_portfolio_signals_for_customer_filters():
    rows = portfolio_signals_for_customer(
        [
            {"customer": "Acme", "signal": "No active users in 7d"},
            {"customer": "Beta", "signal": "Fine"},
        ],
        "Acme",
    )
    assert len(rows) == 1
    assert rows[0]["severity"] >= 1
