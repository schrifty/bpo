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
    """Unmapped cached rows are passed through (on-slide placeholder stays short)."""
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


def test_element_may_contain_data_nps_negative_score():
    """Regression: adapt must consider metric lines with negative numbers (e.g. NPS)."""
    assert evaluate._element_may_contain_data({"text": "NPS: -19", "type": "shape"}) is True


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


def test_element_may_contain_data_spelled_or_quarter():
    """Spelled counts, quarters, months, or 'percent' without digits still qualify."""
    assert evaluate._element_may_contain_data({"text": "twelve active sites"}) is True
    assert evaluate._element_may_contain_data({"text": "January summary"}) is True
    assert evaluate._element_may_contain_data({"text": "Q3 overview"}) is True
    assert evaluate._element_may_contain_data({"text": "growth percent"}) is True


# ── adapt cache / prompt helpers ─────────────────────────────────────────────


def test_data_summary_fingerprint_stable_and_sensitive():
    """Same summary → same fingerprint; different values → different fingerprint."""
    a = evaluate._build_data_summary({"customer": "X", "account": {"total_sites": 1}})
    b = evaluate._build_data_summary({"customer": "X", "account": {"total_sites": 2}})
    assert evaluate._data_summary_fingerprint(a) == evaluate._data_summary_fingerprint(a)
    assert evaluate._data_summary_fingerprint(a) != evaluate._data_summary_fingerprint(b)


def test_format_data_summary_for_adapt_prompt_bounded():
    """Prompt JSON is compact and within max length (no blind mid-slice of top-level JSON)."""
    report = {"customer": "C", "account": {}, "sites": [{"sitename": f"S{i}", "visitors": i} for i in range(100)]}
    s = evaluate._build_data_summary(report)
    out = evaluate._format_data_summary_for_adapt_prompt(s)
    assert len(out) <= 12000
    assert "site_details" in out


def test_normalize_and_dedupe_replacements():
    """Invalid rows dropped; duplicate originals deduped."""
    raw = [
        {"original": "x", "new_value": "1", "mapped": True, "field": "a"},
        "not-a-dict",
        {"original": "x", "new_value": "2", "mapped": True, "field": "b"},
        {"new_value": "missing original"},
    ]
    norm = evaluate._normalize_adapt_replacements(raw)
    assert len(norm) == 2
    assert norm[0]["original"] == "x"
    deduped = evaluate._dedupe_replacements_by_original(norm)
    assert len(deduped) == 1
    assert deduped[0]["new_value"] == "2"


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
    """Data ask keys not in data_summary produce generic [???] on-slide (details in speaker notes)."""
    data_ask = [{"key": "nps_score", "example_from_slide": "72"}]
    data_summary = {}
    text_elements = [{"text": "72"}]
    out = evaluate._resolve_data_ask_to_replacements(data_ask, data_summary, text_elements)
    assert len(out) == 1
    assert out[0]["mapped"] is False
    assert out[0]["new_value"] == "[???]"


def test_resolve_data_ask_embedded_chart():
    """_embedded_chart produces static placeholder replacement."""
    data_ask = [{"key": "_embedded_chart", "example_from_slide": "(embedded chart — ...)"}]
    out = evaluate._resolve_data_ask_to_replacements(data_ask, {}, [])
    assert len(out) == 1
    assert "[CHART" in out[0]["new_value"] and out[0]["mapped"] is False


def test_ensure_charts_and_images_marked():
    """Charts and images get a replacement entry so they are always marked."""
    text_els = [
        {"type": "chart", "element_id": "c1", "text": evaluate._EMBEDDED_CHART_TEXT},
        {"type": "image", "element_id": "i1", "text": "(embedded image)"},
        {"type": "shape", "element_id": "s1", "text": "14 sites"},
    ]
    out = evaluate._ensure_charts_and_images_marked(text_els, [])
    assert len(out) == 2  # one chart, one image
    assert any(r.get("field") == "chart" and not r.get("mapped") for r in out)
    assert any(r.get("field") == "image" and not r.get("mapped") for r in out)
    # If LLM already returned one chart, we add only the missing one
    with_chart = evaluate._ensure_charts_and_images_marked(
        text_els, [{"field": "chart", "original": evaluate._EMBEDDED_CHART_TEXT, "mapped": False}]
    )
    assert sum(1 for r in with_chart if r.get("field") == "chart") == 1
    assert sum(1 for r in with_chart if r.get("field") == "image") == 1


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


def test_build_hydrate_speaker_notes_narrative_no_replacements():
    """Slides with no pipeline ops still get QA notes + slide copy (not template fluff)."""
    els = [
        {"type": "shape", "text": "Prior quarter — Goals & Key Actions"},
        {"type": "shape", "text": "• Ship feature X\n• Improve adoption"},
    ]
    out = evaluate._build_hydrate_speaker_notes([], els, report={"customer": "Acme"})
    assert "No automated data replacements" in out
    assert "Slide copy" in out
    assert "Prior quarter" in out
    assert "Ship feature" in out


def test_build_hydrate_speaker_notes_chart_specs():
    """When analysis has charts[], speaker notes include chart type, axes, transformations, and configuration."""
    reps = [
        {"field": "chart", "original": evaluate._EMBEDDED_CHART_TEXT, "new_value": "[CHART — ...]", "mapped": False},
    ]
    els = [{"type": "chart", "element_id": "c1", "text": evaluate._EMBEDDED_CHART_TEXT}]
    analysis = {
        "charts": [
            {
                "chart_type": "line",
                "x_axis": "Month",
                "y_axis": "Value",
                "transformations": ["rolling 7-day average", "group by site"],
                "configuration": "legend bottom, blue/green series",
            }
        ]
    }
    out = evaluate._build_hydrate_speaker_notes(reps, els, analysis=analysis)
    assert "Visuals —" in out
    assert "Type: line" in out
    assert "X: Month" in out
    assert "Y: Value" in out
    assert "Transforms:" in out
    assert "rolling" in out or "group by site" in out
    assert "Config:" in out and "legend" in out


def test_build_hydrate_speaker_notes_chart_includes_pipeline_snapshot():
    """Chart analysis can list data_recommended_keys; speaker notes attach pipeline values when present."""
    reps = [
        {"field": "chart", "original": evaluate._EMBEDDED_CHART_TEXT, "new_value": "[CHART — ...]", "mapped": False},
    ]
    els = [{"type": "chart", "element_id": "c1", "text": evaluate._EMBEDDED_CHART_TEXT}]
    analysis = {
        "charts": [
            {
                "visual_kind": "native_chart",
                "interpretation": "Column chart of users by site.",
                "chart_type": "column",
                "x_axis": "Site",
                "y_axis": "Users",
                "transformations": [],
                "configuration": "",
                "data_recommended_keys": ["active_users", "total_sites", "bogus_key"],
                "data_coverage_note": "Per-site user counts vs total footprint.",
            }
        ]
    }
    ds = {"active_users": 42, "total_sites": 7}
    out = evaluate._build_hydrate_speaker_notes(
        reps, els, analysis=analysis, data_summary=ds
    )
    assert "Pipeline fields that may supply" in out
    assert "active_users" in out and "total_sites" in out
    assert "bogus_key" not in out
    assert "Data we have for this run" in out
    assert "42" in out and "7" in out
    assert "Pendo" in out
    assert "What it shows:" in out and "Column chart" in out
    assert "Per-site user" in out or "footprint" in out


def test_build_hydrate_speaker_notes_visual_no_pipeline_keys_shows_gap():
    """When LLM cannot map a visual to pipeline keys, notes explain manual sourcing."""
    reps = [
        {"field": "chart", "original": evaluate._EMBEDDED_CHART_TEXT, "new_value": "[CHART — ...]", "mapped": False},
    ]
    els = [{"type": "chart", "element_id": "c1", "text": evaluate._EMBEDDED_CHART_TEXT}]
    analysis = {
        "charts": [
            {
                "visual_kind": "image_or_screenshot",
                "interpretation": "Line graph of export API calls by week; last 90 days.",
                "chart_type": "line",
                "x_axis": "Week",
                "y_axis": "Exports",
                "data_recommended_keys": [],
                "data_coverage_note": "Export usage not in pipeline — not auto-fetchable.",
            }
        ]
    }
    out = evaluate._build_hydrate_speaker_notes(reps, els, analysis=analysis, data_summary={})
    assert "What it shows:" in out and "export" in out.lower()
    assert "Pipeline fields: (none matched" in out
    assert "Auto-fetch: not mapped" in out


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


# ── incomplete banner gating ────────────────────────────────────────────────────


def test_should_add_incomplete_banner_skips_title_slide():
    reps = [{"original": "x", "new_value": "[???]", "mapped": False, "field": "n"}]
    assert evaluate._should_add_incomplete_banner("t1", reps, title_slide_object_id="t1") is False
    assert evaluate._should_add_incomplete_banner("t2", reps, title_slide_object_id="t1") is True


def test_should_add_incomplete_banner_skips_visual_only_unmapped():
    reps = [
        {
            "original": evaluate._EMBEDDED_IMAGE_TEXTS[0],
            "new_value": "",
            "mapped": False,
            "field": "image",
        },
    ]
    assert evaluate._should_add_incomplete_banner("p1", reps, title_slide_object_id=None) is False


def test_should_add_incomplete_banner_true_when_mixed_visual_and_text():
    reps = [
        {
            "original": evaluate._EMBEDDED_IMAGE_TEXTS[0],
            "new_value": "",
            "mapped": False,
            "field": "image",
        },
        {"original": "99", "new_value": "[???]", "mapped": False, "field": "n"},
    ]
    assert evaluate._should_add_incomplete_banner("p1", reps, title_slide_object_id=None) is True


def test_should_add_incomplete_banner_skips_prose_heading_plus_static_image():
    """Section dividers: static image row + LLM false-positive unmapped headline should not get a banner."""
    reps = [
        {
            "original": evaluate._EMBEDDED_IMAGE_TEXTS[0],
            "new_value": evaluate._IMAGE_MARKER,
            "mapped": False,
            "field": "image",
        },
        {
            "original": "Review LeanDNA value areas (top opportunities)",
            "new_value": "[???]",
            "mapped": False,
            "field": "n",
        },
    ]
    assert evaluate._should_add_incomplete_banner("p1", reps, None, None) is False


def test_should_add_incomplete_banner_skips_bespoke_divider_from_analysis():
    reps = [{"original": "x", "new_value": "[???]", "mapped": False, "field": "n"}]
    assert evaluate._should_add_incomplete_banner(
        "p1", reps, None, {"slide_type": "bespoke_divider"}
    ) is False


# ── intake: Drive query escape ──────────────────────────────────────────────────


def test_drive_query_escape_apostrophe():
    assert evaluate._drive_query_escape("a'b") == "a\\'b"
