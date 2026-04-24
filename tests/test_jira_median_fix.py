"""Test median calculation fix for TTFR/TTR SLA metrics."""
import pytest


def test_compute_sla_median_even_length():
    """Verify median calculation works correctly for even-length lists."""
    from src.jira_client import JiraClient
    
    # Even-length list: [4h, 6h, 8h, 10h] -> median should be 7.0h
    issues = [
        {"project": "HELP", "ttfr_ms": 4 * 3600 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
        {"project": "HELP", "ttfr_ms": 6 * 3600 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
        {"project": "HELP", "ttfr_ms": 8 * 3600 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
        {"project": "HELP", "ttfr_ms": 10 * 3600 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
    ]
    
    result = JiraClient._compute_sla(issues, "ttfr")
    
    # Median of [4h, 6h, 8h, 10h] = (6h + 8h) / 2 = 7.0h
    expected_median_ms = 7.0 * 3600 * 1000
    assert result["median_ms"] == expected_median_ms
    assert result["median"] == "7.0h"
    assert result["measured"] == 4


def test_compute_sla_median_odd_length():
    """Verify median calculation works correctly for odd-length lists."""
    from src.jira_client import JiraClient
    
    # Odd-length list: [4h, 6h, 8h] -> median should be 6h
    issues = [
        {"project": "HELP", "ttr_ms": 4 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
        {"project": "HELP", "ttr_ms": 6 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
        {"project": "HELP", "ttr_ms": 8 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
    ]
    
    result = JiraClient._compute_sla(issues, "ttr")
    
    # Median of [4h, 6h, 8h] = 6h
    expected_median_ms = 6.0 * 3600 * 1000
    assert result["median_ms"] == expected_median_ms
    assert result["median"] == "6.0h"
    assert result["measured"] == 3


def test_compute_sla_average():
    """Verify average calculation is correct."""
    from src.jira_client import JiraClient
    
    issues = [
        {"project": "HELP", "ttfr_ms": 2 * 3600 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
        {"project": "HELP", "ttfr_ms": 4 * 3600 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
        {"project": "HELP", "ttfr_ms": 6 * 3600 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
    ]
    
    result = JiraClient._compute_sla(issues, "ttfr")
    
    # Average of [2h, 4h, 6h] = 4h
    expected_avg_ms = 4 * 3600 * 1000
    assert result["avg_ms"] == expected_avg_ms
    assert result["avg"] == "4.0h"


def test_compute_sla_median_vs_old_incorrect_calculation():
    """Demonstrate the fix: old calculation was wrong for even-length lists."""
    from src.jira_client import JiraClient
    
    # Example from the bug report: [4h, 6h, 8h, 10h]
    issues = [
        {"project": "HELP", "ttr_ms": 4 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
        {"project": "HELP", "ttr_ms": 6 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
        {"project": "HELP", "ttr_ms": 8 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
        {"project": "HELP", "ttr_ms": 10 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
    ]
    
    result = JiraClient._compute_sla(issues, "ttr")
    
    # Old incorrect calculation: values[len(values) // 2] = values[2] = 8.0h
    incorrect_median_ms = 8.0 * 3600 * 1000
    
    # New correct calculation: (6h + 8h) / 2 = 7.0h
    correct_median_ms = 7.0 * 3600 * 1000
    
    assert result["median_ms"] == correct_median_ms
    assert result["median_ms"] != incorrect_median_ms
    assert result["median"] == "7.0h"


def test_compute_sla_formats():
    """Verify time formatting (minutes, hours, days) works correctly."""
    from src.jira_client import JiraClient
    
    # Test minutes format
    issues_mins = [
        {"project": "HELP", "ttfr_ms": 30 * 60 * 1000, "ttfr_breached": False, "ttfr_waiting": False},
    ]
    result_mins = JiraClient._compute_sla(issues_mins, "ttfr")
    assert result_mins["median"] == "30 min"
    
    # Test hours format
    issues_hours = [
        {"project": "HELP", "ttr_ms": 5 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
    ]
    result_hours = JiraClient._compute_sla(issues_hours, "ttr")
    assert result_hours["median"] == "5.0h"
    
    # Test days format
    issues_days = [
        {"project": "HELP", "ttr_ms": 48 * 3600 * 1000, "ttr_breached": False, "ttr_waiting": False},
    ]
    result_days = JiraClient._compute_sla(issues_days, "ttr")
    assert result_days["median"] == "2.0d"
