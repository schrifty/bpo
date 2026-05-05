"""Unit tests for optional LLM churn/account-risk insights in the all-customers export."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from src import export_llm_risk_insights as eri


@pytest.fixture
def sample_report() -> dict:
    csr_ph = {
        "sites": [
            {"csr_customer": "Acme", "factory": "F1", "shortages": 3, "health_score": "YELLOW"},
            {"csr_customer": "Beta", "factory": "X", "shortages": 0},
        ]
    }
    return {
        "customers": [
            {
                "customer": "Acme",
                "pendo_csm": "Pat",
                "total_users": 100,
                "active_users": 20,
                "login_pct": 25.0,
                "engagement": {"active_rate_7d": 25.0},
                "kei": {"adoption_rate": 10.0},
                "guides": {"dismiss_rate": 5.0},
            },
            {
                "customer": "Beta",
                "pendo_csm": "",
                "total_users": 50,
                "active_users": 40,
                "login_pct": 88.0,
                "engagement": {"active_rate_7d": 88.0},
            },
        ],
        "portfolio_signals": [
            {"customer": "Acme", "signal": "Low login rate vs peers"},
            {"customer": "Other", "signal": "ignore"},
        ],
        "salesforce": {
            "accounts": [
                {
                    "Name": "Acme",
                    "ARR__c": 1.2e6,
                    "days_until_contract_end_nearest": 45,
                    "active_in_salesforce": True,
                }
            ]
        },
        "csr": {"platform_health": csr_ph, "supply_chain": csr_ph, "platform_value": csr_ph},
    }


def test_build_customer_risk_payloads_merges_domains(sample_report: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(eri, "_env_int", lambda _name, default: 40)

    called: list[str] = []

    class _FakeJira:
        def get_customer_jira(self, name: str, days: int) -> dict:
            called.append(name)
            return {"total_issues": 3, "open_issues": 1, "resolved_issues": 2}

    monkeypatch.setattr(
        "src.jira_client.get_shared_jira_client",
        lambda: _FakeJira(),
    )

    payloads, warns = eri.build_customer_risk_payloads(sample_report, jira_days=30, jira_workers=2)
    assert not warns
    assert len(payloads) == 2
    acme = next(p for p in payloads if p["customer"] == "Acme")
    assert acme["pendo"]["login_pct"] == 25.0
    assert acme["salesforce"].get("days_until_contract_end_nearest") == 45
    assert acme["pendo_portfolio_signals_sample"]
    assert "leandna_data_api" in acme and "note" in acme["leandna_data_api"]
    assert set(called) == {"Acme", "Beta"}
    assert acme["jira_help"].get("total_issues") == 3


def test_risk_insights_enabled_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BPO_LLM_EXPORT_RISK_INSIGHTS", raising=False)
    assert eri.risk_insights_enabled(False) is False
    monkeypatch.setenv("BPO_LLM_EXPORT_RISK_INSIGHTS", "1")
    assert eri.risk_insights_enabled(False) is True
    assert eri.risk_insights_enabled(True) is True


def test_call_risk_llm_batch_parses_customers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test")
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    fake_msg = MagicMock()
    fake_msg.content = json.dumps(
        {
            "customers": [
                {
                    "customer": "Acme",
                    "insights": [
                        {
                            "title": "Renewal window",
                            "detail": "Contract end in 45 days with flat adoption.",
                            "risk_level": "medium",
                            "evidence": ["salesforce.days_until_contract_end_nearest"],
                        },
                        {
                            "title": "Engagement",
                            "detail": "Login rate below peer cohort.",
                            "risk_level": "low",
                            "evidence": ["pendo.login_pct"],
                        },
                    ],
                }
            ]
        }
    )
    fake_choice = MagicMock()
    fake_choice.message = fake_msg
    fake_resp = MagicMock()
    fake_resp.choices = [fake_choice]

    client = MagicMock()
    client.chat.completions.create.return_value = fake_resp
    monkeypatch.setattr("src.config.llm_client", lambda: client)

    rows, err = eri._call_risk_llm_batch(
        [{"customer": "Acme", "pendo": {}, "salesforce": {}}],
        model="gpt-4o-mini",
    )
    assert err is None
    assert len(rows) == 1
    assert len(rows[0]["insights"]) == 2


def test_render_section_surfaces_batch_error(sample_report: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        eri,
        "build_customer_risk_payloads",
        lambda *_a, **_kw: ([{"customer": "Acme", "pendo": {}}], []),
    )
    monkeypatch.setattr(
        eri,
        "_call_risk_llm_batch",
        lambda *_a, **_k: ([], "model timeout"),
    )

    md = eri.render_risk_insights_section(sample_report, jira_days=30, model="gpt-4o-mini")
    assert "## 7. Account & churn risk insights (LLM)" in md
    assert "### Error (partial or failed LLM run)" in md
    assert "model timeout" in md
    assert "### Acme" in md
