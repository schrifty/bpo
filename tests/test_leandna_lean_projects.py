"""Unit tests for LeanDNA Lean Projects API client and enrichment."""
from __future__ import annotations

from unittest.mock import patch, MagicMock
import pytest


# ── Client Tests ──

def test_aggregate_portfolio_stats_empty():
    """Test portfolio stats with empty projects list."""
    from src.leandna_lean_projects_client import aggregate_portfolio_stats
    
    result = aggregate_portfolio_stats([])
    
    assert result["total_projects"] == 0
    assert result["active_projects"] == 0
    assert result["total_savings_actual"] == 0.0
    assert result["total_savings_target"] == 0.0
    assert result["savings_achievement_pct"] == 0.0


def test_aggregate_portfolio_stats_with_data():
    """Test portfolio stats with sample projects."""
    from src.leandna_lean_projects_client import aggregate_portfolio_stats
    
    projects = [
        {
            "id": "PROJ-1",
            "stage": "Execution",
            "state": "good",
            "totalActualSavingsForPeriod": 100000.0,
            "totalTargetSavingsForPeriod": 80000.0,
            "isBestPractice": True,
            "isProjectResultsValidated": True,
        },
        {
            "id": "PROJ-2",
            "stage": "Planning",
            "state": "warn",
            "totalActualSavingsForPeriod": 50000.0,
            "totalTargetSavingsForPeriod": 60000.0,
            "isBestPractice": False,
            "isProjectResultsValidated": False,
        },
        {
            "id": "PROJ-3",
            "stage": "Closed",
            "state": "good",
            "totalActualSavingsForPeriod": 75000.0,
            "totalTargetSavingsForPeriod": 70000.0,
            "isBestPractice": True,
            "isProjectResultsValidated": True,
        },
    ]
    
    result = aggregate_portfolio_stats(projects)
    
    assert result["total_projects"] == 3
    assert result["active_projects"] == 2  # excludes Closed
    assert result["total_savings_actual"] == 225000.0
    assert result["total_savings_target"] == 210000.0
    assert result["savings_achievement_pct"] == pytest.approx(107.14, rel=0.1)
    assert result["best_practice_count"] == 2
    assert result["validated_results_count"] == 2
    assert result["stage_distribution"] == {"Execution": 1, "Planning": 1, "Closed": 1}
    assert result["state_distribution"] == {"good": 2, "warn": 1}


def test_aggregate_monthly_savings():
    """Test monthly savings aggregation."""
    from src.leandna_lean_projects_client import aggregate_monthly_savings
    
    savings_data = [
        {
            "projectId": "PROJ-1",
            "savings": [
                {"month": "2026-03", "actual": 30000.0, "target": 25000.0, "includeInTotals": True},
                {"month": "2026-02", "actual": 28000.0, "target": 25000.0, "includeInTotals": True},
                {"month": "2026-01", "actual": 27000.0, "target": 25000.0, "includeInTotals": False},  # excluded
            ],
        },
        {
            "projectId": "PROJ-2",
            "savings": [
                {"month": "2026-03", "actual": 20000.0, "target": 30000.0, "includeInTotals": True},
                {"month": "2026-02", "actual": 22000.0, "target": 30000.0, "includeInTotals": True},
            ],
        },
    ]
    
    result = aggregate_monthly_savings(savings_data, months=3)
    
    assert len(result) == 2  # only 2 months have data
    assert result[0]["month"] == "2026-03"
    assert result[0]["actual"] == 50000.0  # 30k + 20k
    assert result[0]["target"] == 55000.0  # 25k + 30k
    assert result[1]["month"] == "2026-02"
    assert result[1]["actual"] == 50000.0  # 28k + 22k
    assert result[1]["target"] == 55000.0


def test_get_top_projects_by_savings():
    """Test project sorting by savings."""
    from src.leandna_lean_projects_client import get_top_projects_by_savings
    
    projects = [
        {"id": "PROJ-1", "totalActualSavingsForPeriod": 50000.0},
        {"id": "PROJ-2", "totalActualSavingsForPeriod": 150000.0},
        {"id": "PROJ-3", "totalActualSavingsForPeriod": 100000.0},
        {"id": "PROJ-4", "totalActualSavingsForPeriod": None},
    ]
    
    result = get_top_projects_by_savings(projects, max_projects=2)
    
    assert len(result) == 2
    assert result[0]["id"] == "PROJ-2"  # highest savings
    assert result[1]["id"] == "PROJ-3"


@patch("src.leandna_lean_projects_client.requests.get")
@patch("src.leandna_lean_projects_client._get_bearer_token")
def test_get_lean_projects_success(mock_token, mock_get):
    """Test successful projects fetch."""
    from src.leandna_lean_projects_client import get_lean_projects
    
    mock_token.return_value = "fake_token"
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"id": "PROJ-1", "name": "Project 1"},
        {"id": "PROJ-2", "name": "Project 2"},
    ]
    mock_get.return_value = mock_response
    
    result = get_lean_projects(sites="100,200", date_from="2026-01-01", date_to="2026-03-31", force_refresh=True)
    
    assert len(result) == 2
    assert result[0]["id"] == "PROJ-1"
    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert "dateFrom" in call_args.kwargs["params"]
    assert call_args.kwargs["params"]["dateFrom"] == "2026-01-01"


@patch("src.leandna_lean_projects_client.requests.get")
@patch("src.leandna_lean_projects_client._get_bearer_token")
def test_get_project_savings_success(mock_token, mock_get):
    """Test successful savings fetch."""
    from src.leandna_lean_projects_client import get_project_savings
    
    mock_token.return_value = "fake_token"
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "projectId": "PROJ-1",
            "savings": [
                {"month": "2026-03", "actual": 30000.0, "target": 25000.0},
            ],
        },
    ]
    mock_get.return_value = mock_response
    
    result = get_project_savings(["PROJ-1", "PROJ-2"], force_refresh=True)
    
    assert len(result) == 1
    assert result[0]["projectId"] == "PROJ-1"
    mock_get.assert_called_once()


@patch("src.leandna_lean_projects_client.requests.get")
@patch("src.leandna_lean_projects_client._get_bearer_token")
def test_get_lean_projects_api_error(mock_token, mock_get):
    """Test graceful handling of API errors."""
    from src.leandna_lean_projects_client import get_lean_projects
    import requests
    
    mock_token.return_value = "fake_token"
    mock_get.side_effect = requests.RequestException("Network error")
    
    result = get_lean_projects(force_refresh=True)
    
    assert result == []


# ── Enrichment Tests ──

@patch("src.leandna_lean_projects_enrich.get_lean_projects")
@patch("src.leandna_lean_projects_enrich.get_project_savings")
def test_enrich_qbr_with_lean_projects_success(mock_savings, mock_projects, monkeypatch):
    """Test successful QBR enrichment with Lean Projects."""
    from src.leandna_lean_projects_enrich import enrich_qbr_with_lean_projects
    
    # Mock token
    monkeypatch.setattr("src.leandna_lean_projects_enrich.LEANDNA_DATA_API_BEARER_TOKEN", "fake_token")
    
    # Mock projects
    mock_projects.return_value = [
        {
            "id": "PROJ-1",
            "name": "Test Project",
            "stage": "Execution",
            "state": "good",
            "totalActualSavingsForPeriod": 100000.0,
            "totalTargetSavingsForPeriod": 80000.0,
            "isBestPractice": True,
            "isProjectResultsValidated": True,
            "projectManager": {"name": "Jane Doe"},
        },
    ]
    
    # Mock savings
    mock_savings.return_value = [
        {
            "projectId": "PROJ-1",
            "savings": [
                {"month": "2026-03", "actual": 35000.0, "target": 27000.0, "includeInTotals": True},
                {"month": "2026-02", "actual": 33000.0, "target": 27000.0, "includeInTotals": True},
                {"month": "2026-01", "actual": 32000.0, "target": 26000.0, "includeInTotals": True},
            ],
        },
    ]
    
    report = {
        "customer": "TestCorp",
        "quarter_start": "2026-01-01",
        "quarter_end": "2026-03-31",
    }
    
    result = enrich_qbr_with_lean_projects(report, "TestCorp")
    
    assert "leandna_lean_projects" in result
    enrichment = result["leandna_lean_projects"]
    
    assert enrichment["enabled"] is True
    assert enrichment["total_projects"] == 1
    assert enrichment["active_projects"] == 1
    assert enrichment["total_savings_actual"] == 100000.0
    assert enrichment["total_savings_target"] == 80000.0
    assert enrichment["savings_achievement_pct"] == 125.0
    assert enrichment["best_practice_count"] == 1
    assert len(enrichment["top_projects"]) == 1
    assert len(enrichment["monthly_savings"]) == 3
    
    mock_projects.assert_called_once()
    mock_savings.assert_called_once()


def test_enrich_qbr_without_token(monkeypatch):
    """Test enrichment skips when token is not configured."""
    from src.leandna_lean_projects_enrich import enrich_qbr_with_lean_projects
    
    # Mock token as None
    monkeypatch.setattr("src.leandna_lean_projects_enrich.LEANDNA_DATA_API_BEARER_TOKEN", None)
    
    report = {"customer": "TestCorp"}
    
    result = enrich_qbr_with_lean_projects(report, "TestCorp")
    
    assert "leandna_lean_projects" in result
    assert result["leandna_lean_projects"]["enabled"] is False
    assert result["leandna_lean_projects"]["reason"] == "bearer_token_not_configured"


@patch("src.leandna_lean_projects_enrich.get_lean_projects")
def test_enrich_qbr_no_projects_found(mock_projects, monkeypatch):
    """Test enrichment when no projects are found."""
    from src.leandna_lean_projects_enrich import enrich_qbr_with_lean_projects
    
    monkeypatch.setattr("src.leandna_lean_projects_enrich.LEANDNA_DATA_API_BEARER_TOKEN", "fake_token")
    mock_projects.return_value = []
    
    report = {
        "customer": "TestCorp",
        "quarter_start": "2026-01-01",
        "quarter_end": "2026-03-31",
    }
    
    result = enrich_qbr_with_lean_projects(report, "TestCorp")
    
    assert "leandna_lean_projects" in result
    enrichment = result["leandna_lean_projects"]
    
    assert enrichment["enabled"] is True
    assert enrichment["total_projects"] == 0
    assert enrichment["active_projects"] == 0
    assert enrichment["error"] == "no_projects_for_period"


def test_format_lean_projects_speaker_notes():
    """Test speaker notes formatting."""
    from src.leandna_lean_projects_enrich import format_lean_projects_speaker_notes_supplement
    
    enrichment = {
        "enabled": True,
        "total_projects": 5,
        "active_projects": 4,
        "total_savings_actual": 250000.0,
        "total_savings_target": 200000.0,
        "savings_achievement_pct": 125.0,
        "best_practice_count": 2,
        "validated_results_count": 3,
        "stage_distribution": {"Execution": 3, "Planning": 2},
        "top_projects": [
            {"name": "Project Alpha", "savings_actual": 100000.0, "stage": "Execution"},
            {"name": "Project Beta", "savings_actual": 80000.0, "stage": "Planning"},
        ],
    }
    
    result = format_lean_projects_speaker_notes_supplement(enrichment)
    
    assert "**Total Projects:** 5 (4 active)" in result
    assert "$250,000 actual vs $200,000 target (125.0%)" in result
    assert "**Best Practices:** 2 projects" in result
    assert "**Validated Results:** 3 projects" in result
    assert "Execution: 3" in result
    assert "Project Alpha" in result


def test_format_lean_projects_speaker_notes_disabled():
    """Test speaker notes returns empty when disabled."""
    from src.leandna_lean_projects_enrich import format_lean_projects_speaker_notes_supplement
    
    enrichment = {"enabled": False}
    
    result = format_lean_projects_speaker_notes_supplement(enrichment)
    
    assert result == ""
