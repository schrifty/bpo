"""Tests for the CS-Report-derived business-unit join (src/csr_business_unit_map.py)."""

from __future__ import annotations

from src.csr_business_unit_map import (
    assign_site_business_unit,
    assign_sites,
    build_csr_division_keys,
    emit_bu_rules_yaml,
    normalize_division,
)


def _safran_csr_rows() -> list[dict]:
    return [
        {"customer": "Safran Cabin and Seats", "factoryName": "Montreal CG0", "entity": "Montreal CG0"},
        {"customer": "Safran Cabin and Seats", "factoryName": "Marysville CM1 Engineered Materials Main", "entity": ""},
        {"customer": "Safran Cabin and Seats", "factoryName": "Astronautics", "entity": ""},
        {"customer": "Safran Electrical and Power", "factoryName": "AMX SM1", "entity": "AMX"},
        {"customer": "Safran Aerosystems", "factoryName": "A1P - Chateaudun Production", "entity": ""},
        {"customer": "Safran Electronics and Defense", "factoryName": "Besancon", "entity": "Besancon"},
        {"customer": "Safran SA", "factoryName": "Safran SA Lean Projects", "entity": ""},
        # Soliman appears under BOTH E&P and E&D -> ambiguous key
        {"customer": "Safran Electrical and Power", "factoryName": "Soliman", "entity": "Soliman"},
        {"customer": "Safran Electronics and Defense", "factoryName": "Soliman", "entity": "Soliman"},
    ]


def test_normalize_division_collapses_safran_values() -> None:
    assert normalize_division("Safran", "Safran Cabin and Seats") == "Cabin & Seats"
    assert normalize_division("Safran", "Safran Cabin Water and Waste") == "Cabin & Seats"
    assert normalize_division("Safran", "Safran Electrical and Power") == "Electrical & Power"
    assert normalize_division("Safran", "Safran Electrical and Power Sandbox") == "Electrical & Power"
    assert normalize_division("Safran", "Safran Aerosystems") == "Aerosystems"
    assert normalize_division("Safran", "Safran Electronics and Defense") == "Electronics & Defense"
    assert normalize_division("Safran", "Safran SA") == "Other / Corporate"


def test_normalize_division_generic_uses_residual() -> None:
    # Unknown customer with no normalizer: division is the residual after the prefix.
    assert normalize_division("Acme", "Acme Widgets Division") == "Widgets Division"
    assert normalize_division("Acme", "Acme") is None


def test_build_csr_division_keys_flags_ambiguous() -> None:
    keys = build_csr_division_keys(_safran_csr_rows(), "Safran")
    assert keys["montreal"] == {"Cabin & Seats"}
    assert keys["amx"] == {"Electrical & Power"}
    # Soliman claimed by two divisions -> ambiguous
    assert keys["soliman"] == {"Electrical & Power", "Electronics & Defense"}


def test_build_csr_division_keys_ignores_other_customers() -> None:
    # A different customer with a colliding location token must not pollute the keys.
    rows = _safran_csr_rows() + [
        {"customer": "Hussmann Corporation", "factoryName": "Montreal Plant", "entity": "Kirkland Montreal"},
    ]
    keys = build_csr_division_keys(rows, "Safran")
    assert keys["montreal"] == {"Cabin & Seats"}


def test_assign_site_csr_confirms_location_only_site() -> None:
    keys = build_csr_division_keys(_safran_csr_rows(), "Safran")
    a = assign_site_business_unit("Safran Montreal CG1", "Safran", keys)
    assert a["business_unit"] == "Cabin & Seats"
    assert a["confidence"] == "high"
    assert a["source"] == "csr"


def test_assign_site_csr_corrects_amx_to_electrical_and_power() -> None:
    keys = build_csr_division_keys(_safran_csr_rows(), "Safran")
    a = assign_site_business_unit("Safran AMX SM3", "Safran", keys)
    assert a["business_unit"] == "Electrical & Power"
    assert a["source"] == "csr"


def test_assign_site_ambiguous_csr_key_is_inferred() -> None:
    keys = build_csr_division_keys(_safran_csr_rows(), "Safran")
    # "Safran Xyz Soliman" has no self-label and Soliman is claimed by two divisions.
    a = assign_site_business_unit("Safran Plant Soliman 99Z", "Safran", keys)
    assert a["confidence"] == "inferred"
    assert a["source"] == "csr_ambiguous"
    assert set(a["candidates"]) == {"Electrical & Power", "Electronics & Defense"}


def test_assign_site_self_label_wins_over_ambiguous_csr() -> None:
    keys = build_csr_division_keys(_safran_csr_rows(), "Safran")
    # Self-labels Electrical and Power AND matches ambiguous Soliman key -> name resolves it.
    a = assign_site_business_unit("Safran Electrical and Power Soliman", "Safran", keys)
    assert a["business_unit"] == "Electrical & Power"
    assert a["confidence"] == "high"


def test_assign_site_name_vs_csr_conflict_flagged() -> None:
    # Pendo name says Aerosystems, but CSR key 'montreal' says Cabin & Seats.
    keys = build_csr_division_keys(_safran_csr_rows(), "Safran")
    a = assign_site_business_unit("Safran Aerosystems Montreal", "Safran", keys)
    assert a["confidence"] == "inferred"
    assert a["source"] == "name_vs_csr_conflict"


def test_assign_site_unmatched_falls_through() -> None:
    keys = build_csr_division_keys(_safran_csr_rows(), "Safran")
    a = assign_site_business_unit("Safran Mystery Plant ZZ9", "Safran", keys)
    assert a["business_unit"] is None
    assert a["confidence"] == "none"
    assert a["source"] == "unmatched"


def test_assign_sites_dedupes_and_emits_yaml() -> None:
    rows = _safran_csr_rows()
    sites = ["Safran Montreal CG1", "Safran Montreal CG1", "Safran AMX SM1", "Safran Mystery ZZ9"]
    assignments = assign_sites(sites, "Safran", rows)
    assert len(assignments) == 3  # deduped
    frag = emit_bu_rules_yaml(assignments, "Safran")
    assert "Safran:" in frag
    assert 'business_unit: "Cabin & Seats"' in frag
    assert 'business_unit: "Electrical & Power"' in frag
    # Unmatched site surfaced as a comment, not a live rule
    assert "# " in frag and "Mystery" in frag
