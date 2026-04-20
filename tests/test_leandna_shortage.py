"""Tests for LeanDNA Material Shortage Trends client and enrichment (no network)."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from src import leandna_shortage_client as client
from src import leandna_shortage_enrich as enrich


@pytest.fixture
def mock_weekly_shortage_data():
    """Sample weekly shortage data for testing."""
    return [
        {
            "itemCode": "ITEM-001",
            "itemDescription": "Critical Component A",
            "site": "Plant A",
            "criticalityLevel": 5,
            "daysInShortage": 12,
            "ctbShortageImpactedValue": 120000.0,
            "firstCriticalBucketWeek": "2026-04-28",
            "buyer": "John Doe",
            "planner": "Jane Smith",
            "buckets": [
                {"week_num": 1, "quantity": 100.0, "start_date": "2026-04-21", "end_date": "2026-04-27", "criticality": "High"},
                {"week_num": 2, "quantity": 200.0, "start_date": "2026-04-28", "end_date": "2026-05-04", "criticality": "Critical"},
                {"week_num": 3, "quantity": 150.0, "start_date": "2026-05-05", "end_date": "2026-05-11", "criticality": "Critical"},
            ],
        },
        {
            "itemCode": "ITEM-002",
            "itemDescription": "Standard Part B",
            "site": "Plant B",
            "criticalityLevel": 3,
            "daysInShortage": 8,
            "ctbShortageImpactedValue": 45000.0,
            "firstCriticalBucketWeek": "2026-05-05",
            "buckets": [
                {"week_num": 1, "quantity": 50.0, "start_date": "2026-04-21", "end_date": "2026-04-27", "criticality": "Medium"},
                {"week_num": 2, "quantity": 75.0, "start_date": "2026-04-28", "end_date": "2026-05-04", "criticality": "Medium"},
                {"week_num": 3, "quantity": 100.0, "start_date": "2026-05-05", "end_date": "2026-05-11", "criticality": "Critical"},
            ],
        },
        {
            "itemCode": "ITEM-003",
            "itemDescription": "Low-Risk Part C",
            "site": "Plant A",
            "criticalityLevel": 2,
            "daysInShortage": 3,
            "ctbShortageImpactedValue": 5000.0,
            "buckets": [
                {"week_num": 1, "quantity": 20.0, "start_date": "2026-04-21", "end_date": "2026-04-27", "criticality": "Low"},
                {"week_num": 2, "quantity": 30.0, "start_date": "2026-04-28", "end_date": "2026-05-04", "criticality": "Low"},
            ],
        },
    ]


def test_normalize_weekly_buckets():
    """Test bucket normalization from flat fields to list."""
    row = {
        "bucket1quantity": 100.0,
        "bucket1startDate": "2026-04-21",
        "bucket1endDate": "2026-04-27",
        "bucket1criticality": "High",
        "bucket2quantity": 200.0,
        "bucket2startDate": "2026-04-28",
        "bucket2endDate": "2026-05-04",
        "bucket2criticality": "Critical",
    }
    buckets = client._normalize_weekly_buckets(row)
    assert len(buckets) == 2
    assert buckets[0]["week_num"] == 1
    assert buckets[0]["quantity"] == 100.0
    assert buckets[0]["criticality"] == "High"
    assert buckets[1]["week_num"] == 2
    assert buckets[1]["quantity"] == 200.0


def test_aggregate_shortage_forecast(mock_weekly_shortage_data):
    """Test shortage forecast aggregation."""
    result = client.aggregate_shortage_forecast(mock_weekly_shortage_data, weeks_forward=3)
    
    assert result["total_items"] == 3
    assert result["critical_items"] == 2  # items with criticalityLevel >= 3
    
    buckets = result["buckets"]
    assert len(buckets) == 3  # 3 weeks
    
    # Week 1 totals
    assert buckets[0]["week_start"] == "2026-04-21"
    assert buckets[0]["total_qty"] == 170.0  # 100 + 50 + 20
    assert buckets[0]["critical_items"] == 0  # no critical items in week 1
    assert buckets[0]["high_items"] == 1
    assert buckets[0]["medium_items"] == 1
    assert buckets[0]["low_items"] == 1
    
    # Week 2 should have critical items
    assert buckets[1]["critical_items"] == 1  # ITEM-001 is critical
    
    # Peak week should be week 2 (highest total qty)
    assert result["peak_week"] == "2026-04-28"
    
    # Total shortage value
    assert result["total_shortage_value"] == 170000  # 120k + 45k + 5k


def test_aggregate_shortage_forecast_empty_data():
    """Test forecast with empty data."""
    result = client.aggregate_shortage_forecast([])
    assert result["total_items"] == 0
    assert result["critical_items"] == 0
    assert result["buckets"] == []
    assert result["peak_week"] is None


def test_get_critical_shortages_timeline(mock_weekly_shortage_data):
    """Test critical shortage timeline extraction."""
    result = client.get_critical_shortages_timeline(mock_weekly_shortage_data, threshold=3, max_items=20)
    
    assert len(result) == 2  # ITEM-001 and ITEM-002 (criticalityLevel >= 3)
    
    # Should be sorted by CTB impact descending
    assert result[0]["itemCode"] == "ITEM-001"
    assert result[0]["ctbImpact"] == 120000
    assert result[0]["firstCriticalWeek"] == "2026-04-28"
    
    assert result[1]["itemCode"] == "ITEM-002"
    assert result[1]["ctbImpact"] == 45000


def test_get_critical_shortages_timeline_respects_max_items(mock_weekly_shortage_data):
    """Test max_items limit."""
    result = client.get_critical_shortages_timeline(mock_weekly_shortage_data, threshold=2, max_items=2)
    assert len(result) == 2  # limited to 2


def test_get_scheduled_deliveries_summary():
    """Test scheduled deliveries summary."""
    delivery_data = [
        {
            "itemCode": "ITEM-001",
            "scheduledDeliveries": 3,
            "scheduledQuantity": 500.0,
            "firstDeliveryDate": "2026-04-22T10:00:00Z",
            "firstDeliveryQty": 200.0,
        },
        {
            "itemCode": "ITEM-002",
            "scheduledDeliveries": 2,
            "scheduledQuantity": 300.0,
            "firstDeliveryDate": "2026-04-25T14:00:00Z",
            "firstDeliveryQty": 150.0,
        },
        {
            "itemCode": "ITEM-003",
            # no scheduled deliveries
        },
    ]
    
    result = client.get_scheduled_deliveries_summary(delivery_data, next_n_days=7)
    
    assert result["items_with_schedules"] == 2
    assert result["avg_deliveries_per_item"] == 2.5  # (3 + 2) / 2
    # next_7_qty depends on date parsing; should include both items if within 7 days


@patch("src.leandna_shortage_enrich.LEANDNA_DATA_API_BEARER_TOKEN", "test_token")
@patch("src.leandna_shortage_enrich.get_shortages_by_item_weekly")
def test_enrich_qbr_with_shortage_trends(mock_get_weekly, mock_weekly_shortage_data):
    """Test QBR enrichment with shortage trends."""
    mock_get_weekly.return_value = mock_weekly_shortage_data
    
    report = {"customer": "TestCorp"}
    result = enrich.enrich_qbr_with_shortage_trends(report, "TestCorp", weeks_forward=12)
    
    assert result["leandna_shortage_trends"]["enabled"] is True
    assert result["leandna_shortage_trends"]["total_items_in_shortage"] == 3
    assert result["leandna_shortage_trends"]["critical_items"] == 2
    
    forecast = result["leandna_shortage_trends"]["forecast"]
    assert len(forecast["buckets"]) == 3  # limited by available data
    assert forecast["peak_week"] == "2026-04-28"
    assert forecast["total_shortage_value"] == 170000
    
    critical_timeline = result["leandna_shortage_trends"]["critical_timeline"]
    assert len(critical_timeline) == 2
    assert critical_timeline[0]["itemCode"] == "ITEM-001"


@patch("src.leandna_shortage_enrich.LEANDNA_DATA_API_BEARER_TOKEN", "test_token")
@patch("src.leandna_shortage_enrich.get_shortages_by_item_weekly")
def test_enrich_qbr_with_shortage_trends_no_data(mock_get_weekly):
    """Test enrichment with no shortage data."""
    mock_get_weekly.return_value = []
    
    report = {"customer": "TestCorp"}
    result = enrich.enrich_qbr_with_shortage_trends(report, "TestCorp")
    
    assert result["leandna_shortage_trends"]["enabled"] is True
    assert result["leandna_shortage_trends"]["total_items_in_shortage"] == 0
    assert "error" in result["leandna_shortage_trends"]


@patch("src.leandna_shortage_enrich.LEANDNA_DATA_API_BEARER_TOKEN", None)
def test_enrich_qbr_without_bearer_token():
    """Test enrichment skips when bearer token not configured."""
    report = {"customer": "TestCorp"}
    result = enrich.enrich_qbr_with_shortage_trends(report, "TestCorp")
    
    assert result["leandna_shortage_trends"]["enabled"] is False
    assert result["leandna_shortage_trends"]["reason"] == "bearer_token_not_configured"


@patch("src.leandna_shortage_enrich.LEANDNA_DATA_API_BEARER_TOKEN", "test_token")
@patch("src.leandna_shortage_enrich.get_shortages_by_item_weekly")
def test_enrich_qbr_handles_api_error_gracefully(mock_get_weekly):
    """Test graceful error handling."""
    mock_get_weekly.side_effect = Exception("API error")
    
    report = {"customer": "TestCorp"}
    result = enrich.enrich_qbr_with_shortage_trends(report, "TestCorp")
    
    assert result["leandna_shortage_trends"]["enabled"] is True
    assert "error" in result["leandna_shortage_trends"]
    assert "API error" in result["leandna_shortage_trends"]["error"]


def test_format_shortage_speaker_notes_supplement(mock_weekly_shortage_data):
    """Test speaker notes formatting."""
    report = {
        "leandna_shortage_trends": {
            "enabled": True,
            "total_items_in_shortage": 3,
            "critical_items": 2,
            "forecast": {
                "buckets": [],
                "peak_week": "2026-04-28",
                "total_shortage_value": 170000,
            },
            "critical_timeline": [
                {"itemCode": "ITEM-001", "ctbImpact": 120000, "firstCriticalWeek": "2026-04-28"},
                {"itemCode": "ITEM-002", "ctbImpact": 45000, "firstCriticalWeek": "2026-05-05"},
            ],
            "scheduled_deliveries": {
                "items_with_schedules": 2,
                "next_n_days_scheduled_qty": 500.0,
            },
        }
    }
    
    notes = enrich.format_shortage_speaker_notes_supplement(report)
    
    assert "3 items in shortage" in notes
    assert "2 critical" in notes
    assert "Peak shortage week is 2026-04-28" in notes
    assert "$170,000" in notes
    assert "ITEM-001" in notes
    assert "2 items have confirmed PO schedules" in notes


def test_format_shortage_speaker_notes_disabled():
    """Test speaker notes when shortage trends disabled."""
    report = {"leandna_shortage_trends": {"enabled": False}}
    notes = enrich.format_shortage_speaker_notes_supplement(report)
    assert notes == ""
