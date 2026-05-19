"""CS Report customer column alias matching (Pendo name vs export `customer`)."""
import json
from unittest.mock import patch

from src import cs_report_client


def test_normalize_health_score_from_json_kpi() -> None:
    raw = json.dumps({"endValue": "GREEN", "empty": False})
    assert cs_report_client._normalize_health_score(raw) == "GREEN"


@patch.object(cs_report_client, "_fetch_latest_report")
def test_customer_rows_matches_alias(mock_fetch: object) -> None:
    """When Pendo/customer name differs from CS export name, alias rows resolve."""
    mock_fetch.return_value = [
        {
            "customer": "Example Manufacturing",
            "delta": "week",
            "factoryName": "Plant A",
            "healthScore": "GREEN",
        },
    ]
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value={"abc": ["Example Manufacturing"]}):
        rows = cs_report_client._customer_rows("ABC", "week")
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
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value={"abc": ["B", "A"]}):
        c = cs_report_client.cs_report_customer_name_candidates("ABC")
    assert c == ["ABC", "B", "A"]


@patch.object(cs_report_client, "_fetch_latest_report")
def test_sites_for_customer_lookup_tries_salesforce_label_and_aliases(mock_fetch: object) -> None:
    """SF rollup 'Johnson' should resolve when CS export uses a legal name via aliases."""
    mock_fetch.return_value = [
        {
            "customer": "Johnson Controls",
            "delta": "week",
            "factoryName": "Plant 1",
            "healthScore": "GREEN",
        },
    ]
    alias_map = {
        "johnson": ["Johnson Controls", "Johnson Controls International", "JCI"],
        "jci": ["Johnson Controls", "Johnson Controls International"],
    }
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value=alias_map):
        with patch.object(cs_report_client, "_load_cohort_customer_alias_map", return_value={}):
            rows, matched, tried, merged = cs_report_client._sites_for_customer_lookup(
                "Johnson",
                lookup_keys=["Johnson", "JCI"],
            )
    assert len(rows) == 1
    assert matched == "Johnson"
    assert "Johnson Controls" in tried
    assert merged == ["Johnson Controls"]


@patch.object(cs_report_client, "_fetch_latest_report")
def test_sites_for_customer_lookup_merges_multiple_csr_customer_names(mock_fetch: object) -> None:
    mock_fetch.return_value = [
        {"customer": "Johnson Controls", "delta": "week", "factoryName": "A", "healthScore": "GREEN"},
        {
            "customer": "Johnson Controls International",
            "delta": "week",
            "factoryName": "B",
            "healthScore": "GREEN",
        },
    ]
    alias_map = {
        "johnson": ["Johnson Controls", "Johnson Controls International"],
    }
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value=alias_map):
        with patch.object(cs_report_client, "_load_cohort_customer_alias_map", return_value={}):
            rows, matched, tried, merged = cs_report_client._sites_for_customer_lookup("Johnson")
    assert len(rows) == 2
    assert set(merged) == {"Johnson Controls", "Johnson Controls International"}


@patch.object(cs_report_client, "_fetch_latest_report")
def test_load_csr_top_customers_uses_sf_label_lookup_keys(mock_fetch: object) -> None:
    mock_fetch.return_value = [
        {"customer": "Johnson Controls", "delta": "week", "factoryName": "F1", "healthScore": "GREEN"},
    ]
    alias_map = {"johnson": ["Johnson Controls"], "jci": ["Johnson Controls"]}
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value=alias_map):
        with patch.object(cs_report_client, "_load_cohort_customer_alias_map", return_value={}):
            out = cs_report_client.load_csr_top_customers_by_arr(
                [
                    {
                        "salesforce_label": "Johnson",
                        "arr": 1_500_000.0,
                        "pendo_customer_key": "JCI",
                        "csr_lookup_name": "JCI",
                    }
                ]
            )
    assert "Johnson" in out["customers"]
    block = out["customers"]["Johnson"]
    assert block["csr_matched_lookup_key"] == "Johnson"
    assert not block["platform_health"].get("error")
