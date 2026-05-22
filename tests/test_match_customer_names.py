"""Unit tests for cross-system customer name resolution."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MATCH_CUSTOMER_NAMES_PATH = _REPO_ROOT / "src" / "match-customer-names.py"
_SPEC = importlib.util.spec_from_file_location(
    "match_customer_names", _MATCH_CUSTOMER_NAMES_PATH
)
assert _SPEC is not None and _SPEC.loader is not None
match_customer_names = importlib.util.module_from_spec(_SPEC)
sys.modules["match_customer_names"] = match_customer_names
_SPEC.loader.exec_module(match_customer_names)

NO_MATCH_LABEL = match_customer_names.NO_MATCH_LABEL
customers_with_missing_matches = match_customer_names.customers_with_missing_matches
render_match_report_text = match_customer_names.render_match_report_text
resolve_pendo_name = match_customer_names.resolve_pendo_name


def test_resolve_pendo_uses_sf_alias_file_only_after_sf_miss(monkeypatch):
    prefixes = frozenset({"JCI", "Spirit"})
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._load_sf_portfolio_pendo_alias_map",
        lambda: {"johnson": ["JCI"]},
    )
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._resolve_sf_label_via_pendo_alias_file",
        lambda sf_label, canon: "JCI" if sf_label == "Johnson" else None,
    )
    monkeypatch.setattr(
        match_customer_names,
        "_pendo_heuristic_on_sf_label",
        lambda sf_label, pendo_prefixes: None,
    )

    name, notes = resolve_pendo_name("Johnson", prefixes)
    assert name == "JCI"
    assert notes and "sf_portfolio_pendo_aliases" in notes[0]


def test_pendo_display_prefers_matching_sitename_for_acuna() -> None:
    prefixes = frozenset({"Current"})
    sites = {
        "Current": [
            "Current Lighting Tijuana",
            "Current Lighting Acuna",
            "Current Lighting Acuna Del Rio",
        ],
    }
    name, notes = match_customer_names.resolve_pendo_name(
        "Current Lighting - Acuna",
        prefixes,
        pendo_sites_by_prefix=sites,
    )
    assert name == "Current Lighting Acuna"
    assert any("sitename match" in n for n in notes)


def test_resolve_pendo_direct_prefix_before_alias(monkeypatch):
    prefixes = frozenset({"Safran"})
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._load_sf_portfolio_pendo_alias_map",
        lambda: {"safran": ["Other"]},
    )
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._resolve_sf_label_via_pendo_alias_file",
        lambda sf_label, canon: "Other",
    )

    name, notes = resolve_pendo_name("Safran", prefixes)
    assert name == "Safran"
    assert notes == []


def test_text_report_shows_pendo_csr_jsm_and_no_match_label() -> None:
    text = render_match_report_text(
        {
            "salesforce_configured": True,
            "pendo_prefix_count": 1,
            "csr_customer_count": 1,
            "jsm_org_count": 0,
            "total": 1,
            "sources_queried": {"pendo": True, "csr": True, "jsm": True},
            "by_status": {
                "active": [
                    {
                        "salesforce_label": "Acme",
                        "contract_statuses": [],
                        "arr": None,
                        "pendo_name": None,
                        "csr_names": [],
                        "jsm_names": [],
                        "alias_notes": [],
                    }
                ],
                "churned": [],
                "renewal_in_negotiation": [],
            },
        }
    )
    assert "  Pendo: (no match)" in text
    assert "  CSR: (no match)" in text
    assert "  JSM: (no match)" in text
    assert text.count(NO_MATCH_LABEL) == 3


def test_partial_matches_summary_lists_customers_at_end() -> None:
    report = {
        "salesforce_configured": True,
        "pendo_prefix_count": 1,
        "csr_customer_count": 1,
        "jsm_org_count": 1,
        "total": 2,
        "sources_queried": {"pendo": True, "csr": True, "jsm": True},
        "by_status": {
            "active": [
                {
                    "salesforce_label": "Acme",
                    "contract_statuses": [],
                    "arr": None,
                    "pendo_name": "Acme",
                    "csr_names": [],
                    "jsm_names": ["Acme Org"],
                    "alias_notes": [],
                },
                {
                    "salesforce_label": "Zebra",
                    "contract_statuses": [],
                    "arr": None,
                    "pendo_name": "Zebra",
                    "csr_names": ["Zebra"],
                    "jsm_names": ["Zebra Org"],
                    "alias_notes": [],
                },
            ],
            "churned": [],
            "renewal_in_negotiation": [],
        },
    }
    text = render_match_report_text(report)
    assert "=== Customers with at least one missing match (1) ===" in text
    assert "  Acme  [active]  — no match: CSR" in text
    assert "Zebra" not in text.split("=== Customers with at least one missing match")[1]

    summary = customers_with_missing_matches(report)
    assert len(summary) == 1
    assert summary[0]["salesforce_label"] == "Acme"
    assert summary[0]["missing"] == ["CSR"]


def test_pendo_stale_alias_lists_no_match(monkeypatch) -> None:
    prefixes = frozenset({"JCI"})
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._load_sf_portfolio_pendo_alias_map",
        lambda: {"acme": ["Ghost", "JCI"]},
    )
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._resolve_sf_label_via_pendo_alias_file",
        lambda sf_label, canon: canon.get("jci"),
    )
    monkeypatch.setattr(
        match_customer_names,
        "_pendo_heuristic_on_sf_label",
        lambda sf_label, pendo_prefixes: None,
    )

    name, notes = resolve_pendo_name("Acme", prefixes)
    assert name == "JCI"
    assert any("Ghost" in n and "not in Pendo customer list" in n for n in notes)


def test_pendo_alias_only_stale_targets_no_match(monkeypatch) -> None:
    prefixes = frozenset({"JCI"})
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._load_sf_portfolio_pendo_alias_map",
        lambda: {"acme": ["Ghost"]},
    )
    monkeypatch.setattr(
        "src.portfolio_salesforce_allowlist._resolve_sf_label_via_pendo_alias_file",
        lambda sf_label, canon: None,
    )
    monkeypatch.setattr(
        match_customer_names,
        "_pendo_heuristic_on_sf_label",
        lambda sf_label, pendo_prefixes: None,
    )

    name, notes = resolve_pendo_name("Acme", prefixes)
    assert name is None
    assert any("Ghost" in n and "not in Pendo customer list" in n for n in notes)


def test_csr_stale_alias_note_without_match(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.cs_report_client._load_cs_report_alias_map",
        lambda: {"acme": ["Ghost CSR"]},
    )
    monkeypatch.setattr(
        "src.cs_report_client._load_cohort_customer_alias_map",
        lambda: {},
    )
    monkeypatch.setattr(
        "src.cs_report_client._sites_for_customer_lookup",
        lambda key, **kwargs: ([], None, [], []),
    )

    matched, notes = match_customer_names.resolve_csr_names(
        "Acme", "Acme", csr_names=frozenset({"Real CSR"})
    )
    assert matched == []
    assert any("Ghost CSR" in n and "not in CS Report customer list" in n for n in notes)


def test_csr_sf_match_skips_alias_lookup(monkeypatch) -> None:
    calls: list[str] = []

    def _lookup(key: str, **kwargs):
        calls.append(key)
        if key == "Acme":
            return ([], None, [], ["Acme CSR"])
        return ([], None, [], [])

    monkeypatch.setattr(
        "src.cs_report_client._load_cs_report_alias_map",
        lambda: {"acme": ["Ghost CSR"]},
    )
    monkeypatch.setattr(
        "src.cs_report_client._sites_for_customer_lookup",
        _lookup,
    )

    matched, notes = match_customer_names.resolve_csr_names(
        "Acme", None, csr_names=frozenset({"Acme CSR", "Ghost CSR"})
    )
    assert matched == ["Acme CSR"]
    assert calls == ["Acme"]


def test_jsm_stale_alias_does_not_false_match() -> None:
    with patch(
        "src.jira_client._load_jsm_org_alias_map",
        return_value={"acme": ["Ghost Org", "Acme Org"]},
    ):
        matched, notes = match_customer_names.resolve_jsm_names(
            "Acme",
            None,
            jsm_orgs=["Acme Org"],
        )
    assert matched == ["Acme Org"]
    assert any("Ghost Org" in n and "not in JSM organization directory" in n for n in notes)
    assert any(
        "jsm_organization_aliases.yaml" in n and "Acme Org" in n for n in notes
    )


def test_jsm_sf_exact_match_skips_alias_terms() -> None:
    with patch(
        "src.jira_client._load_jsm_org_alias_map",
        return_value={"acme": ["Other Org"]},
    ):
        matched, notes = match_customer_names.resolve_jsm_names(
            "Acme",
            None,
            jsm_orgs=["Acme"],
        )
    assert matched == ["Acme"]
    assert not any("Other Org" in n for n in notes)
