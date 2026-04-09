"""Slide YAML rollup_params readers (benchmarks, cohort profiles, cohort findings)."""

from src.slide_loader import (
    benchmarks_min_peers_for_cohort_median,
    cohort_findings_metadata,
    cohort_findings_rollup_params,
    cohort_findings_min_customers_for_cross_cohort_compare,
    cohort_profiles_max_physical_slides,
    get_slide_definition,
)


def test_benchmarks_min_peers_matches_repo_yaml():
    assert benchmarks_min_peers_for_cohort_median() == 3


def test_cohort_profiles_max_slides_matches_repo_yaml():
    assert cohort_profiles_max_physical_slides() == 10


def test_cohort_findings_metadata_templates_from_yaml():
    md = cohort_findings_metadata()
    assert md["max_bullets"] == 1
    assert md["priority"][0] == "single_bucket"
    assert "provenance" in md["templates"]
    assert "{names}" in md["templates"]["singleton_one"]


def test_cohort_findings_rollup_has_expected_keys_and_defaults():
    p = cohort_findings_rollup_params()
    assert p["min_customers_for_cross_cohort_compare"] == 5
    assert p["min_login_spread_pp"] == 5
    assert p["singleton_n"] == 1
    assert p["thin_sample_n"] == 2
    assert cohort_findings_min_customers_for_cross_cohort_compare() == 5


def test_get_slide_definition_qbr_agenda_includes_hydrate():
    sd = get_slide_definition("qbr_agenda")
    assert sd is not None
    assert sd.get("id") == "qbr_agenda"
    h = sd.get("hydrate")
    assert isinstance(h, dict)
    st = h.get("template", {}).get("section_titles", {})
    assert st.get("from_deck_plan") is True
    assert st.get("slot_labels") == "title_number_hash"
