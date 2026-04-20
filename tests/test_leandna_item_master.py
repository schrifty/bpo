"""Tests for LeanDNA Item Master Data client and enrichment (no network)."""
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from src import leandna_item_master_client as client
from src import leandna_item_master_enrich as enrich


@pytest.fixture
def mock_items():
    """Sample Item Master Data for testing."""
    return [
        {
            "itemCode": "ITEM-001",
            "itemDescription": "Critical Component A",
            "site": "Plant A",
            "aggregateRiskScore": 95,
            "daysOfInventoryBackward": 72.0,
            "daysOfInventoryForward": 45.0,
            "abcRank": "A",
            "leadTime": 14.0,
            "observedLeadTime": 35.0,
            "excessOnHandValue": 50000.0,
            "excessOnHandQty": 1200.0,
            "ctbShortageImpactedValue": 120000.0,
        },
        {
            "itemCode": "ITEM-002",
            "itemDescription": "Standard Part B",
            "site": "Plant A",
            "aggregateRiskScore": 45,
            "daysOfInventoryBackward": 30.0,
            "daysOfInventoryForward": 28.0,
            "abcRank": "B",
            "leadTime": 7.0,
            "observedLeadTime": 8.0,
            "excessOnHandValue": 5000.0,
            "excessOnHandQty": 200.0,
        },
        {
            "itemCode": "ITEM-003",
            "itemDescription": "Low-Value Part C",
            "site": "Plant B",
            "aggregateRiskScore": 85,
            "daysOfInventoryBackward": 120.0,
            "daysOfInventoryForward": 15.0,
            "abcRank": "C",
            "leadTime": 21.0,
            "observedLeadTime": 18.0,
            "excessOnHandValue": 2000.0,
            "excessOnHandQty": 500.0,
        },
    ]


def test_get_high_risk_items_filters_by_threshold(mock_items):
    result = client.get_high_risk_items(mock_items, threshold=80, max_items=10)
    assert len(result) == 2
    assert result[0]["aggregateRiskScore"] == 95
    assert result[1]["aggregateRiskScore"] == 85


def test_get_high_risk_items_respects_max_items(mock_items):
    result = client.get_high_risk_items(mock_items, threshold=40, max_items=2)
    assert len(result) == 2


def test_get_doi_backwards_summary_calculates_stats(mock_items):
    result = client.get_doi_backwards_summary(mock_items)
    assert result["total_items_with_doi_bwd"] == 3
    assert result["mean"] == 74.0  # (72 + 30 + 120) / 3
    assert result["median"] == 72.0
    assert result["min"] == 30.0
    assert result["max"] == 120.0
    assert result["items_over_60_days"] == 2  # ITEM-001 and ITEM-003


def test_get_doi_backwards_summary_handles_missing_values():
    items = [
        {"itemCode": "A", "daysOfInventoryBackward": 50.0},
        {"itemCode": "B"},  # missing DOI bwd
        {"itemCode": "C", "daysOfInventoryBackward": None},
    ]
    result = client.get_doi_backwards_summary(items)
    assert result["total_items_with_doi_bwd"] == 1
    assert result["mean"] == 50.0


def test_get_abc_distribution_counts_by_rank(mock_items):
    result = client.get_abc_distribution(mock_items)
    assert result["A"] == 1
    assert result["B"] == 1
    assert result["C"] == 1
    assert result["Unknown"] == 0


def test_get_abc_distribution_handles_unknown(mock_items):
    items = mock_items + [{"itemCode": "X", "abcRank": ""}, {"itemCode": "Y"}]
    result = client.get_abc_distribution(items)
    assert result["Unknown"] == 2


def test_get_lead_time_variance_calculates_pct(mock_items):
    result = client.get_lead_time_variance(mock_items, min_variance_pct=20.0)
    assert len(result) == 1  # Only ITEM-001: (35-14)/14 = 150%
    assert result[0]["itemCode"] == "ITEM-001"
    assert result[0]["variance_pct"] == 150.0


def test_get_lead_time_variance_filters_by_supplier(mock_items):
    items = [
        {"itemCode": "A", "supplier": "Acme", "leadTime": 10.0, "observedLeadTime": 20.0},
        {"itemCode": "B", "supplier": "XYZ", "leadTime": 10.0, "observedLeadTime": 25.0},
    ]
    result = client.get_lead_time_variance(items, supplier="Acme", min_variance_pct=20.0)
    assert len(result) == 1
    assert result[0]["itemCode"] == "A"


def test_get_excess_items_returns_top_by_value(mock_items):
    items, total = client.get_excess_items(mock_items, max_items=2)
    assert len(items) == 2
    assert items[0]["itemCode"] == "ITEM-001"  # $50k
    assert items[1]["itemCode"] == "ITEM-002"  # $5k
    assert total == 57000.0  # sum of all three


def test_get_excess_items_filters_positive_only():
    items = [
        {"itemCode": "A", "excessOnHandValue": 1000.0},
        {"itemCode": "B", "excessOnHandValue": 0.0},
        {"itemCode": "C", "excessOnHandValue": -500.0},
        {"itemCode": "D"},  # missing
    ]
    result, total = client.get_excess_items(items)
    assert len(result) == 1
    assert result[0]["itemCode"] == "A"
    assert total == 1000.0


@patch("src.leandna_item_master_enrich.get_item_master_data")
def test_enrich_qbr_with_item_master_adds_enrichment(mock_get_data, mock_items, monkeypatch):
    monkeypatch.setattr("src.leandna_item_master_enrich.LEANDNA_DATA_API_BEARER_TOKEN", "test-token")
    mock_get_data.return_value = mock_items
    
    report = {"customer": "Test Customer"}
    result = enrich.enrich_qbr_with_item_master(report, "Test Customer")
    
    assert "leandna_item_master" in result
    ldna = result["leandna_item_master"]
    assert ldna["enabled"] is True
    assert ldna["item_count"] == 3
    assert len(ldna["high_risk_items"]) == 2  # threshold 80
    assert ldna["doi_backwards"]["mean"] == 74.0
    assert ldna["abc_distribution"]["A"] == 1


def test_enrich_qbr_with_item_master_skips_if_not_configured(monkeypatch):
    monkeypatch.setattr("src.leandna_item_master_enrich.LEANDNA_DATA_API_BEARER_TOKEN", None)
    report = {"customer": "Test"}
    result = enrich.enrich_qbr_with_item_master(report, "Test")
    
    assert "leandna_item_master" in result
    assert result["leandna_item_master"]["enabled"] is False
    assert result["leandna_item_master"]["reason"] == "bearer_token_not_configured"


@patch("src.leandna_item_master_enrich.get_item_master_data")
def test_enrich_qbr_handles_api_error_gracefully(mock_get_data, monkeypatch):
    monkeypatch.setattr("src.leandna_item_master_enrich.LEANDNA_DATA_API_BEARER_TOKEN", "test-token")
    mock_get_data.side_effect = Exception("API timeout")
    
    report = {"customer": "Test"}
    result = enrich.enrich_qbr_with_item_master(report, "Test")
    
    assert "leandna_item_master" in result
    assert result["leandna_item_master"]["enabled"] is True
    assert "error" in result["leandna_item_master"]


def test_format_leandna_speaker_notes_supplement_with_data(mock_items):
    enrichment = {
        "enabled": True,
        "item_count": 3,
        "doi_backwards": {"mean": 74.0, "median": 72.0, "items_over_60_days": 2},
        "high_risk_items": [
            {"itemCode": "ITEM-001", "aggregateRiskScore": 95},
            {"itemCode": "ITEM-003", "aggregateRiskScore": 85},
        ],
        "abc_distribution": {"A": 1, "B": 1, "C": 1, "Unknown": 0},
        "excess_breakdown": {
            "total_excess_items": 3,
            "excess_on_hand_value": 57000,
            "top_excess_items": [
                {"itemCode": "ITEM-001", "excessOnHandValue": 50000},
            ],
        },
        "lead_time_variance": {
            "high_variance_count": 1,
            "worst_performers": [
                {"itemCode": "ITEM-001", "variance_pct": 150.0},
            ],
        },
    }
    report = {"leandna_item_master": enrichment}
    
    notes = enrich.format_leandna_speaker_notes_supplement(report)
    
    assert "DOI Backwards" in notes
    assert "74.0 days" in notes
    assert "2 items exceed 60 days" in notes
    assert "High-risk items" in notes
    assert "ITEM-001 (score 95)" in notes
    assert "ABC classification" in notes
    assert "Excess inventory" in notes
    assert "$57,000" in notes
    assert "Lead time variance" in notes


def test_format_leandna_speaker_notes_returns_empty_if_disabled():
    report = {"leandna_item_master": {"enabled": False}}
    notes = enrich.format_leandna_speaker_notes_supplement(report)
    assert notes == ""


def test_format_leandna_speaker_notes_returns_empty_if_error():
    report = {"leandna_item_master": {"enabled": True, "error": "API failed"}}
    notes = enrich.format_leandna_speaker_notes_supplement(report)
    assert notes == ""
