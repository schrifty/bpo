"""CS Report customer column alias matching (Pendo name vs export `customer`)."""
from unittest.mock import patch

from src import cs_report_client


@patch.object(cs_report_client, "_fetch_latest_report")
def test_customer_rows_matches_alias(mock_fetch: object) -> None:
    """When Pendo name is JCI but CS export uses Johnson Controls, rows resolve."""
    mock_fetch.return_value = [
        {
            "customer": "Johnson Controls",
            "delta": "week",
            "factoryName": "Plant A",
            "healthScore": "GREEN",
        },
    ]
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value={"jci": ["Johnson Controls"]}):
        rows = cs_report_client._customer_rows("JCI", "week")
    assert len(rows) == 1
    assert rows[0].get("factoryName") == "Plant A"


@patch.object(cs_report_client, "_fetch_latest_report")
def test_customer_rows_exact_name_without_alias(mock_fetch: object) -> None:
    mock_fetch.return_value = [
        {"customer": "ACME", "delta": "week", "factoryName": "F1"},
    ]
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value={}):
        rows = cs_report_client._customer_rows("ACME", "week")
    assert len(rows) == 1


@patch.object(cs_report_client, "_fetch_latest_report")
def test_cs_report_customer_name_candidates_order(mock_fetch: object) -> None:
    mock_fetch.return_value = []
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value={"jci": ["B", "A"]}):
        c = cs_report_client.cs_report_customer_name_candidates("JCI")
    assert c == ["JCI", "B", "A"]
