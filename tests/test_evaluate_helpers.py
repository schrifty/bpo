"""Tests for evaluate helpers: resolve cache, element filter, data summary, customer detection."""
import pytest

from src import evaluate


# ── _resolve_cached_replacements ──────────────────────────────────────────────


def test_resolve_cached_replacements_mapped_simple():
    """Mapped replacement gets new_value from data_summary key."""
    cached = [
        {"original": "31", "new_value": "14", "mapped": True, "field": "total_sites"},
    ]
    data = {"total_sites": 22}
    out = evaluate._resolve_cached_replacements(cached, data)
    assert len(out) == 1
    assert out[0]["new_value"] == "22"


def test_resolve_cached_replacements_preserves_suffix():
    """Format suffix from original is preserved (e.g. '31 sites' -> '14 sites')."""
    cached = [
        {"original": "31 sites", "new_value": "14 sites", "mapped": True, "field": "total_sites"},
    ]
    data = {"total_sites": 7}
    out = evaluate._resolve_cached_replacements(cached, data)
    assert out[0]["new_value"] == "7 sites"


def test_resolve_cached_replacements_unmapped_unchanged():
    """Unmapped replacement keeps existing new_value (placeholder)."""
    cached = [
        {"original": "TBD", "new_value": "[???]", "mapped": False, "field": "n/a"},
    ]
    data = {}
    out = evaluate._resolve_cached_replacements(cached, data)
    assert out[0]["new_value"] == "[???]"


def test_resolve_cached_replacements_field_normalized():
    """Field key is normalized (spaces/hyphens -> underscores, lowercased)."""
    cached = [
        {"original": "Q1", "new_value": "Q2", "mapped": True, "field": "Quarter"},
    ]
    data = {"quarter": "Q3 2025"}
    out = evaluate._resolve_cached_replacements(cached, data)
    assert out[0]["new_value"] == "Q3 2025"


def test_resolve_cached_replacements_list_value_truncated():
    """List/dict values are stringified and truncated."""
    cached = [
        {"original": "x", "new_value": "y", "mapped": True, "field": "site_details"},
    ]
    data = {"site_details": [{"name": "A", "visitors": 10}] * 50}
    out = evaluate._resolve_cached_replacements(cached, data)
    assert len(out[0]["new_value"]) <= 200


def test_resolve_cached_replacements_none_value():
    """None in data_summary becomes empty string."""
    cached = [
        {"original": "n", "new_value": "1", "mapped": True, "field": "total_users"},
    ]
    data = {"total_users": None}
    out = evaluate._resolve_cached_replacements(cached, data)
    assert out[0]["new_value"] == ""


# ── _element_may_contain_data ─────────────────────────────────────────────────


def test_element_may_contain_data_embedded_image():
    """Elements starting with '(embedded' or '(image' are included."""
    assert evaluate._element_may_contain_data({"text": "(embedded image)"}) is True
    assert evaluate._element_may_contain_data({"text": "(image in shape)"}) is True


def test_element_may_contain_data_has_digit_or_symbol():
    """Text with digits or % $ is considered data."""
    assert evaluate._element_may_contain_data({"text": "31 sites"}) is True
    assert evaluate._element_may_contain_data({"text": "Q1 2025"}) is True
    assert evaluate._element_may_contain_data({"text": "$1.2M"}) is True
    assert evaluate._element_may_contain_data({"text": "42%"}) is True


def test_element_may_contain_data_pure_labels_excluded():
    """Short or non-data text is excluded."""
    assert evaluate._element_may_contain_data({"text": "A"}) is False
    assert evaluate._element_may_contain_data({"text": ""}) is False
    assert evaluate._element_may_contain_data({"text": "Section title"}) is False
    assert evaluate._element_may_contain_data({"text": "Key metrics"}) is False


# ── _build_data_summary ──────────────────────────────────────────────────────


def test_build_data_summary_minimal():
    """Minimal report produces expected flat keys."""
    report = {"customer": "Acme", "account": {}}
    s = evaluate._build_data_summary(report)
    assert s["customer_name"] == "Acme"
    assert s["total_sites"] == 0
    assert s["total_users"] == 0
    assert "quarter" in s
    assert "quarter_start" in s
    assert "quarter_end" in s


def test_build_data_summary_account():
    """Account fields are flattened into summary."""
    report = {
        "customer": "Acme",
        "account": {
            "total_visitors": 100,
            "total_sites": 5,
            "health_score": "GREEN",
        },
    }
    s = evaluate._build_data_summary(report)
    assert s["total_users"] == 100
    assert s["total_sites"] == 5
    assert s["health_score"] == "GREEN"


def test_build_data_summary_sites_capped():
    """Site details are capped (e.g. 30)."""
    report = {
        "sites": [{"sitename": f"S{i}", "visitors": i} for i in range(50)],
    }
    s = evaluate._build_data_summary(report)
    assert len(s["site_details"]) == 30


# ── _detect_customer (list-only branch, no LLM) ───────────────────────────────


def test_detect_customer_match_in_list():
    """Customer name in title is matched from known list."""
    known = ["Safran", "Bombardier"]
    assert evaluate._detect_customer("Safran QBR 2025", known) == "Safran"
    assert evaluate._detect_customer("Bombardier — Review", known) == "Bombardier"


def test_detect_customer_company_name_excluded():
    """LeanDNA/Leandna is never chosen as customer when another candidate exists."""
    known = ["Safran", "Leandna"]
    # "Safran & Leandna" should return Safran (company name excluded from candidates)
    assert evaluate._detect_customer("Safran & Leandna", known) == "Safran"


def test_detect_customer_longest_match():
    """Longest matching candidate is preferred (e.g. 'Safran Aerospace' vs 'Safran')."""
    known = ["Safran", "Safran Aerospace"]
    # sorted by len reverse: "Safran Aerospace" first, then "Safran"
    # "Safran Aerospace QBR" matches "safran aerospace" first
    assert evaluate._detect_customer("Safran Aerospace QBR", known) == "Safran Aerospace"


# ── _resolve_data_ask_to_replacements (broad analysis) ────────────────────────


def test_resolve_data_ask_to_replacements_mapped():
    """Data ask items with keys in data_summary produce mapped replacements."""
    data_ask = [
        {"key": "total_sites", "example_from_slide": "31 sites"},
        {"key": "quarter", "example_from_slide": "Q1 2025"},
    ]
    data_summary = {"total_sites": 14, "quarter": "Q3 2025"}
    text_elements = [{"text": "31 sites"}, {"text": "Q1 2025"}]
    out = evaluate._resolve_data_ask_to_replacements(data_ask, data_summary, text_elements)
    assert len(out) == 2
    assert out[0]["original"] == "31 sites" and out[0]["new_value"] == "14 sites" and out[0]["mapped"]
    assert out[1]["original"] == "Q1 2025" and out[1]["new_value"] == "Q3 2025" and out[1]["mapped"]


def test_resolve_data_ask_to_replacements_unmapped():
    """Data ask keys not in data_summary produce unmapped placeholders."""
    data_ask = [{"key": "nps_score", "example_from_slide": "72"}]
    data_summary = {}
    text_elements = [{"text": "72"}]
    out = evaluate._resolve_data_ask_to_replacements(data_ask, data_summary, text_elements)
    assert len(out) == 1
    assert out[0]["mapped"] is False and out[0]["new_value"] == "[???]"


def test_resolve_data_ask_embedded_chart():
    """_embedded_chart produces static placeholder replacement."""
    data_ask = [{"key": "_embedded_chart", "example_from_slide": "(embedded chart — ...)"}]
    out = evaluate._resolve_data_ask_to_replacements(data_ask, {}, [])
    assert len(out) == 1
    assert "[CHART" in out[0]["new_value"] and out[0]["mapped"] is False


# ── _derive_reproducibility (evaluate: deduce from data_ask) ───────────────────


def test_derive_reproducibility_fully():
    """All data_ask keys available → fully reproducible."""
    analysis = {"data_ask": [{"key": "total_sites"}, {"key": "quarter"}], "slide_type": "custom"}
    out = evaluate._derive_reproducibility(analysis)
    assert out["feasibility"] == "fully reproducible"
    assert out["confidence"] == 100
    assert all(d["available"] for d in out["data_needed"])
    assert len(out["gaps"]) == 0


def test_derive_reproducibility_partially():
    """Some keys available → partially reproducible."""
    analysis = {"data_ask": [{"key": "total_sites"}, {"key": "nps_score"}], "slide_type": "custom"}
    out = evaluate._derive_reproducibility(analysis)
    assert out["feasibility"] == "partially reproducible"
    assert "nps_score" in out["gaps"]


def test_derive_reproducibility_static():
    """No data_ask → fully reproducible (static slide)."""
    analysis = {"data_ask": [], "slide_type": "custom"}
    out = evaluate._derive_reproducibility(analysis)
    assert out["feasibility"] == "fully reproducible"
    assert "Static slide" in out["summary"]


def test_cache_hit_rate_line():
    assert "30/40 (75%)" in evaluate._cache_hit_rate_line("x", 30, 40)
    assert evaluate._cache_hit_rate_line("x", 0, 0) == "x: no slides"


def test_build_hydrate_speaker_notes_lists_lines():
    """Speaker notes manifest includes each replacement and per-line slide data."""
    reps = [
        {"field": "total_sites", "original": "31", "new_value": "14", "mapped": True},
        {"field": "nps_score", "original": "72", "new_value": "[???]", "mapped": False},
    ]
    els = [
        {"type": "shape", "text": "[$000] Manufacturing sites\n> [000] Unique Weekly Visitors"},
        {"type": "shape", "text": "[00%] reduction In Past Due PO's"},
    ]
    out = evaluate._build_hydrate_speaker_notes(reps, els)
    assert "Pipeline" in out and "total_sites" in out and "nps_score" in out
    assert "Manufacturing sites" in out
    assert "Past Due" in out
    assert out.count("[shape]") >= 2


def test_build_hydrate_speaker_notes_qa_governance():
    """Speaker notes include data context, source attribution, and INCOMPLETE when applicable."""
    reps = [
        {"field": "total_sites", "original": "31", "new_value": "14", "mapped": True},
        {"field": "support", "original": "5", "new_value": "12", "mapped": True},
    ]
    els = [{"type": "shape", "text": "14 sites"}]
    report = {"customer": "Acme", "generated": "2025-03-06", "quarter": "Q1 2025"}
    out = evaluate._build_hydrate_speaker_notes(
        reps, els, report=report, has_unmapped=False, has_static_images=False
    )
    assert "QA this slide" in out
    assert "Data context" in out and "Acme" in out and "2025-03-06" in out and "Q1 2025" in out
    assert "LIVE — Source: Pendo" in out
    assert "LIVE — Source: Jira" in out
    assert "QA checklist" in out
    assert "INCOMPLETE" not in out

    out_incomplete = evaluate._build_hydrate_speaker_notes(
        reps, els, report=report, has_unmapped=True, has_static_images=False
    )
    assert "INCOMPLETE" in out_incomplete and "Confirm before presenting" in out_incomplete


def test_build_hydrate_speaker_notes_rebuild_spec():
    """When analysis is provided, notes include objective and required data for rebuild."""
    reps = [{"field": "total_sites", "original": "31", "new_value": "14", "mapped": True}]
    els = [{"type": "shape", "text": "14 sites"}]
    analysis = {
        "purpose": "Account overview with site and user counts.",
        "title": "Account at a glance",
        "slide_type": "engagement",
        "data_ask": [{"key": "total_sites", "example_from_slide": "31 sites"}, {"key": "quarter", "example_from_slide": "Q4 2024"}],
    }
    out = evaluate._build_hydrate_speaker_notes(reps, els, analysis=analysis)
    assert "Objective: Account overview" in out
    assert "Required data: total_sites, quarter" in out
    assert "Slide: engagement — Account at a glance" in out
