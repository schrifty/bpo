"""CS Report customer column alias matching (Pendo name vs export `customer`)."""
import json
from datetime import datetime
from unittest.mock import patch

from src import cs_report_client


def test_build_csr_site_entry_maps_full_csr_row() -> None:
    row = {
        "factoryName": "Plant A",
        "entity": "Entity A",
        "region": "NA",
        "division": "Auto",
        "businessUnit": "Powertrain",
        "healthScore": "GREEN",
        "shortageItemCount": json.dumps({"endValue": 12, "empty": False}),
        "clearToBuildPercent": json.dumps({"endValue": 88.5, "empty": False}),
        "totalOnHandValue": json.dumps({"endValue": 1000000, "empty": False}),
        "dailyInventoryUsage": json.dumps({"endValue": 25000, "empty": False}),
        "excessOnOrderValuePositive": json.dumps({"endValue": 5000, "empty": False}),
        "potentialSavings": json.dumps({"endValue": 9000, "empty": False}),
        "potentialToSell": json.dumps({"endValue": 4000, "empty": False}),
        "apexPoActionPoCt": json.dumps({"endValue": 3, "empty": False}),
        "supplierCommitDatePercent": json.dumps({"endValue": 91.2, "empty": False}),
        "automatedHealthScores": json.dumps([{"healthScore": 92.0, "override": "GREEN"}]),
        "startDate": datetime(2026, 1, 1),
        "endDate": datetime(2026, 12, 31),
    }
    entry = cs_report_client._build_csr_site_entry(row)
    assert entry["factory"] == "Plant A"
    assert entry["health_score"] == "GREEN"
    assert entry["shortages"] == 12
    assert entry["clear_to_build_pct"] == 88.5
    assert entry["on_hand_value"] == 1000000
    assert entry["daily_inventory_usage"] == 25000
    assert entry["excess_on_order_value"] == 5000
    assert entry["potential_savings"] == 9000
    assert entry["potential_to_sell"] == 4000
    assert entry["apex_po_action_po_ct"] == 3
    assert entry["supplier_commit_date_pct"] == 91.2
    assert entry["automated_health_composite"] == 92.0
    assert entry["start_date"] == "2026-01-01"
    assert entry["end_date"] == "2026-12-31"


def test_normalize_health_score_from_json_kpi() -> None:
    raw = json.dumps({"endValue": "GREEN", "empty": False})
    assert cs_report_client._normalize_health_score(raw) == "GREEN"


def test_health_score_none_column_uses_automated_composite() -> None:
    row = {
        "healthScore": "NONE",
        "automatedHealthScores": json.dumps(
            [{"healthScore": 100.0, "override": None, "siteName": "Plant A"}]
        ),
    }
    assert cs_report_client._health_score_from_row(row) == "GREEN"


def test_health_score_none_without_automated_stays_none() -> None:
    row = {"healthScore": "NONE", "factoryName": "JCI USD Conversion"}
    assert cs_report_client._health_score_from_row(row) == "NONE"


@patch.object(cs_report_client, "_fetch_latest_report")
def test_johnson_health_distribution_resolves_none_via_automated(mock_fetch: object) -> None:
    """Johnson Controls export often has healthScore=NONE with automated composite present."""
    mock_fetch.return_value = [
        {"customer": "Johnson Controls", "delta": "week", "factoryName": "Scored", "healthScore": "GREEN"},
        {
            "customer": "Johnson Controls",
            "delta": "week",
            "factoryName": "Auto only",
            "healthScore": "NONE",
            "automatedHealthScores": json.dumps([{"healthScore": 85.0, "override": None}]),
        },
        {"customer": "Johnson Controls", "delta": "week", "factoryName": "Conversion", "healthScore": "NONE"},
    ]
    with patch.object(cs_report_client, "_load_cs_report_alias_map", return_value={"jci": ["Johnson Controls"]}):
        with patch.object(cs_report_client, "_load_cohort_customer_alias_map", return_value={}):
            ph = cs_report_client.get_customer_platform_health("JCI", lookup_keys=["JCI"])
    dist = ph.get("health_distribution") or {}
    assert dist.get("NONE", 0) == 1
    assert dist.get("GREEN", 0) == 2


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
def test_cirtec_alias_matches_csr_workbook_label(mock_fetch: object) -> None:
    mock_fetch.return_value = [
        {"customer": "Cirtec Medical Corp", "delta": "week", "factoryName": "Plant A", "healthScore": "GREEN"},
    ]
    with patch.object(
        cs_report_client,
        "_load_cs_report_alias_map",
        return_value={"cirtec": ["Cirtec Medical Corp"]},
    ):
        with patch.object(cs_report_client, "_load_cohort_customer_alias_map", return_value={}):
            rows, matched, _tried, merged = cs_report_client._sites_for_customer_lookup(
                "Cirtec",
                lookup_keys=cs_report_client.cs_report_lookup_keys_for_account(
                    salesforce_label="Cirtec Medical Corp.",
                    pendo_customer_key="Cirtec",
                ),
            )
    assert len(rows) == 1
    assert matched == "Cirtec"
    assert merged == ["Cirtec Medical Corp"]


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
