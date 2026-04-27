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


def test_format_data_summary_for_adapt_prompt_caps_wide_dicts():
    """Very wide nested dicts must not exceed adapt prompt budget (dict key cap)."""
    s = {
        "customer_name": "Acme",
        "platform_value": {f"k{i}": {"note": "x" * 500, "v": i} for i in range(2000)},
    }
    out = evaluate._format_data_summary_for_adapt_prompt(s)
    assert len(out) <= 12000
    assert "platform_value" in out


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


def test_normalize_adapt_replacements_skips_single_digit_original():
    """replaceAllText('1'→…) hits every '1' on the page — breaks P1/P2 and similar."""
    raw = [
        {"original": "1", "new_value": "[000]", "mapped": False, "field": "bad"},
        {"original": "P1", "new_value": "P1", "mapped": True, "field": "ok"},
    ]
    norm = evaluate._normalize_adapt_replacements(raw)
    assert len(norm) == 1
    assert norm[0]["original"] == "P1"


def test_normalize_adapt_replacements_skips_percent_to_non_percent():
    """Do not apply a replacement when the slide value is a % but the new value is not."""
    raw = [
        {"original": "42%", "new_value": "40", "mapped": True, "field": "bad"},
        {"original": "42%", "new_value": "40%", "mapped": True, "field": "ok"},
        {"original": "12 percent", "new_value": "10", "mapped": True, "field": "bad2"},
        {"original": "12 percent", "new_value": "10 percent", "mapped": True, "field": "ok2"},
    ]
    norm = evaluate._normalize_adapt_replacements(raw)
    assert [r["new_value"] for r in norm] == ["40%", "10 percent"]


def test_sanitize_adapt_replacements_percent_demotes_when_new_value_lacks_percent():
    """Post-synonym guard: % slots must not ship as bare scalars (e.g. weekly hours)."""
    els = [{"type": "shape", "text": "91% of the COGS under management"}]
    repl = [
        {
            "original": "91%",
            "new_value": "39371.5 of the COGS under management",
            "mapped": True,
            "field": "account_avg_weekly_hours",
        }
    ]
    out = evaluate._sanitize_adapt_replacements_percent_semantics(repl, els)
    assert out[0]["mapped"] is False
    assert "[00%]" in out[0]["new_value"]


def test_sanitize_adapt_replacements_percent_detects_bare_digits_before_percent_sign():
    els = [{"type": "shape", "text": "91% of the COGS under management"}]
    repl = [{"original": "91", "new_value": "39371.5", "mapped": True, "field": "x"}]
    out = evaluate._sanitize_adapt_replacements_percent_semantics(repl, els)
    assert out[0]["mapped"] is False


def test_adapt_text_has_percentage_semantics():
    assert evaluate._adapt_text_has_percentage_semantics("42%")
    assert evaluate._adapt_text_has_percentage_semantics("about 5 percent")
    assert evaluate._adapt_text_has_percentage_semantics("[00%]")
    assert not evaluate._adapt_text_has_percentage_semantics("42")
    assert not evaluate._adapt_text_has_percentage_semantics("100")


def test_sanitize_adapt_replacements_plausible_years_demotes_absurd():
    """Minutes/hours misread as calendar years must not ship as mapped."""
    raw = [
        {
            "original": "XX years",
            "new_value": "39375.5 years",
            "mapped": True,
            "field": "account_total_minutes",
        },
        {
            "original": "15 years",
            "new_value": "12 years",
            "mapped": True,
            "field": "tenure",
        },
        {
            "original": "29 sites",
            "new_value": "29 sites",
            "mapped": True,
            "field": "total_sites",
        },
    ]
    out = evaluate._sanitize_adapt_replacements_plausible_years(raw)
    assert len(out) == 3
    assert out[0]["mapped"] is False
    assert out[0]["new_value"] == "[000] years"
    assert "implausible" in out[0]["field"]
    assert out[1]["mapped"] is True
    assert out[1]["new_value"] == "12 years"
    assert out[2]["mapped"] is True


def test_sanitize_adapt_replacements_plausible_years_demotes_wrong_unit_field():
    """Values tied to minutes/hours in field text are demoted even when < 150."""
    raw = [
        {
            "original": "XX years",
            "new_value": "120",
            "mapped": True,
            "field": "account_total_minutes",
        },
    ]
    out = evaluate._sanitize_adapt_replacements_plausible_years(raw)
    assert out[0]["mapped"] is False
    assert out[0]["new_value"] == "[000] years"


# ── _build_data_summary ──────────────────────────────────────────────────────


def test_build_data_summary_csr_nested_matches_legacy_keys():
    """CS Report data may live under report['csr'] or legacy cs_platform_* keys."""
    pv = {"customer": "Acme", "source": "cs_report", "total_savings": 42}
    nested = evaluate._build_data_summary({"customer": "Acme", "account": {}, "csr": {"platform_value": pv}})
    legacy = evaluate._build_data_summary({"customer": "Acme", "account": {}, "cs_platform_value": pv})
    assert nested.get("platform_value") == pv
    assert legacy.get("platform_value") == pv


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
    known = ["ExampleCo", "Sample Industries"]
    assert evaluate._detect_customer("ExampleCo QBR 2025", known) == "ExampleCo"
    assert evaluate._detect_customer("Sample Industries — Review", known) == "Sample Industries"


def test_detect_customer_company_name_excluded():
    """LeanDNA/Leandna is never chosen as customer when another candidate exists."""
    known = ["ExampleCo", "Leandna"]
    # "ExampleCo & Leandna" should return ExampleCo (company name excluded from candidates)
    assert evaluate._detect_customer("ExampleCo & Leandna", known) == "ExampleCo"


def test_detect_customer_longest_match():
    """Longest matching candidate is preferred (e.g. 'Example Aerospace' vs 'Example')."""
    known = ["Example", "Example Aerospace"]
    # sorted by len reverse: "Example Aerospace" first, then "Example"
    # "Example Aerospace QBR" matches "example aerospace" first
    assert evaluate._detect_customer("Example Aerospace QBR", known) == "Example Aerospace"


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


def test_build_hydrate_speaker_notes_generic_placeholder_one_liner():
    """Generic [???]→[???] rows use [generic] unmapped instead of long UNMAPPED tag."""
    reps = [
        {
            "field": "Generic placeholder, no specific data mapping",
            "original": "[???]",
            "new_value": "[???]",
            "mapped": False,
        },
    ]
    out = evaluate._build_hydrate_speaker_notes(reps, [{"type": "shape", "text": "x"}])
    assert "Data Fields:" in out
    assert "generic placeholder — no pipeline mapping" in out
    assert "UNMAPPED / static visual" not in out


def test_build_hydrate_speaker_notes_lists_lines():
    """Speaker notes list each replacement as [slide] -> [field] -> [value] (no duplicate on-slide scan)."""
    reps = [
        {"field": "total_sites", "original": "31", "new_value": "14", "mapped": True},
        {"field": "nps_score", "original": "72", "new_value": "[???]", "mapped": False},
    ]
    els = [
        {"type": "shape", "text": "[$000] Manufacturing sites\n> [000] Unique Weekly Visitors"},
        {"type": "shape", "text": "[00%] reduction In Past Due PO's"},
    ]
    out = evaluate._build_hydrate_speaker_notes(reps, els)
    assert "Data Fields:" in out and "total_sites" in out and "nps_score" in out
    assert "`total_sites`" in out and "Pendo" in out and "[14]" in out
    assert "`nps_score` unmapped" in out and "[72]" in out


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
    assert "QA this slide" not in out
    assert "Data context" in out and "Acme" in out and "2025-03-06" in out and "Q1 2025" in out
    assert "`total_sites`" in out and "Pendo" in out and "[14]" in out
    assert "`support`" in out and "Jira" in out and "[12]" in out
    assert "QA:" in out
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
    assert "No automated data replacements" not in out
    assert "Slide copy (reference):" in out
    assert "YAML:" in out
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
    assert "Visuals:" in out
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
    assert "Pipeline fields (guess):" in out
    assert "active_users" in out and "total_sites" in out
    assert "bogus_key" not in out
    assert "Data snapshot for this run" in out
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
    assert "Not auto-fetchable" in out


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
    assert "Required data keys: total_sites, quarter" in out
    assert "Account at a glance" in out.split("\n")[0]
    assert "Type: engagement" in out


def test_hydrate_speaker_notes_title_skips_placeholder_first_line():
    """Header line uses first non-placeholder shape line, not [???] template slots."""
    els = [{"type": "shape", "text": "[???]\nUsage & engagement"}]
    out = evaluate._build_hydrate_speaker_notes([], els, report={"customer": "Acme"})
    first = out.split("\n")[0]
    assert "— [???]" not in first
    assert "Usage & engagement" in first


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


def test_should_add_incomplete_banner_skips_qbr_divider_from_analysis():
    reps = [{"original": "x", "new_value": "[???]", "mapped": False, "field": "n"}]
    assert evaluate._should_add_incomplete_banner(
        "p1", reps, None, {"slide_type": "qbr_divider"}
    ) is False


def test_slide_metric_font_clamp_requests_lowers_inherited_headline_size():
    """replaceAllText can inherit 72pt headline style on metric text — clamp to body reference."""
    slide = {
        "pageElements": [
            {
                "objectId": "shape1",
                "shape": {
                    "text": {
                        "textElements": [
                            {
                                "textRun": {
                                    "content": "Label ",
                                    "style": {"fontSize": {"magnitude": 12, "unit": "PT"}},
                                }
                            },
                            {
                                "textRun": {
                                    "content": "$166,290",
                                    "style": {"fontSize": {"magnitude": 72, "unit": "PT"}},
                                }
                            },
                        ]
                    }
                }
            }
        ]
    }
    reps = [{"original": "$100,000", "new_value": "$166,290", "mapped": True}]
    reqs = evaluate._slide_metric_font_clamp_requests(slide, reps)
    assert len(reqs) == 1
    mag = reqs[0]["updateTextStyle"]["style"]["fontSize"]["magnitude"]
    assert mag <= 22.0


# ── QBR agenda Title #N → section titles ───────────────────────────────────────


@pytest.mark.slow
def test_merge_qbr_agenda_title_replacements_replaces_title_hash():
    plan = [
        {"slide_type": "qbr_cover", "title": "Cover"},
        {"slide_type": "qbr_agenda", "title": "Agenda"},
        {"slide_type": "qbr_divider", "title": "First Section"},
        {"slide_type": "qbr_divider", "title": "Second Section"},
    ]
    text_elements = [
        {"type": "shape", "element_id": "a", "text": "Agenda"},
        {"type": "shape", "element_id": "b", "text": "Title #1"},
        {"type": "shape", "element_id": "c", "text": "Title #2"},
    ]
    base = [
        {"original": "Title #1", "new_value": "wrong", "mapped": True, "field": "x"},
        {"original": "166290", "new_value": "99", "mapped": True, "field": "y"},
    ]
    out = evaluate._merge_qbr_agenda_title_replacements(text_elements, base, {"_slide_plan": plan})
    by_orig = {r["original"]: r["new_value"] for r in out}
    assert by_orig["Title #1"] == "First Section"
    assert by_orig["Title #2"] == "Second Section"
    assert by_orig["166290"] == "99"


def test_merge_qbr_agenda_title_replacements_no_plan_noop():
    text_elements = [{"type": "shape", "element_id": "b", "text": "Title #1"}]
    base = [{"original": "x", "new_value": "y", "mapped": True, "field": "z"}]
    out = evaluate._merge_qbr_agenda_title_replacements(text_elements, base, {})
    assert out == base


def test_merge_qbr_agenda_title_replacements_yaml_opt_out():
    """YAML hydrate.template.section_titles.from_deck_plan: false disables title merge."""
    plan = [
        {"slide_type": "qbr_divider", "title": "First Section"},
        {"slide_type": "qbr_divider", "title": "Second Section"},
    ]
    text_elements = [
        {"type": "shape", "element_id": "a", "text": "Agenda"},
        {"type": "shape", "element_id": "b", "text": "Title #1"},
    ]
    base = [{"original": "keep", "new_value": "me", "mapped": True, "field": "x"}]
    report = {
        "_slide_plan": plan,
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "section_titles": {"from_deck_plan": False, "slot_labels": "title_number_hash"},
                }
            }
        },
    }
    out = evaluate._merge_qbr_agenda_title_replacements(text_elements, base, report)
    assert out == base


def test_merge_qbr_agenda_title_replacements_with_yaml_hints():
    """Explicit qbr_agenda hints (from slide YAML) still merge when from_deck_plan is true."""
    plan = [
        {"slide_type": "qbr_divider", "title": "Alpha"},
        {"slide_type": "qbr_divider", "title": "Beta"},
    ]
    text_elements = [
        {"type": "shape", "element_id": "a", "text": "Agenda"},
        {"type": "shape", "element_id": "b", "text": "Title #1"},
        {"type": "shape", "element_id": "c", "text": "Title #2"},
    ]
    report = {
        "_slide_plan": plan,
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "section_titles": {"from_deck_plan": True, "slot_labels": "title_number_hash"},
                }
            }
        },
    }
    out = evaluate._merge_qbr_agenda_title_replacements(text_elements, [], report)
    by_orig = {r["original"]: r["new_value"] for r in out}
    assert by_orig["Title #1"] == "Alpha"
    assert by_orig["Title #2"] == "Beta"


def test_merge_qbr_agenda_title_truncates_to_max_chars_per_section_title():
    plan = [
        {"slide_type": "qbr_divider", "title": "This is a very long section title indeed"},
    ]
    text_elements = [
        {"type": "shape", "element_id": "a", "text": "Agenda"},
        {"type": "shape", "element_id": "b", "text": "Title #1"},
    ]
    report = {
        "_slide_plan": plan,
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "section_titles": {
                        "from_deck_plan": True,
                        "slot_labels": "title_number_hash",
                        "max_chars_per_section_title": 20,
                    }
                }
            }
        },
    }
    out = evaluate._merge_qbr_agenda_title_replacements(text_elements, [], report)
    nv = next(x["new_value"] for x in out if x["original"] == "Title #1")
    assert len(nv) <= 20


def test_qbr_agenda_adapt_extra_rules_uses_adapt_instructions_verbatim():
    text_elements = [
        {"type": "shape", "element_id": "a", "text": "Agenda"},
        {"type": "shape", "element_id": "b", "text": "Title #1"},
    ]
    report = {
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "adapt_instructions": "  Line one about agenda.\n\n  Line two.\n",
                    "section_titles": {"max_chars_per_section_title": 99},
                }
            }
        }
    }
    s = evaluate._qbr_agenda_adapt_extra_rules(report, text_elements)
    assert s == "Line one about agenda.\n\n  Line two."
    assert "99" not in s


def test_qbr_agenda_adapt_extra_rules_legacy_when_no_adapt_instructions():
    """Older YAML with only max_chars_* still gets generated prose."""
    text_elements = [
        {"type": "shape", "element_id": "a", "text": "Agenda"},
        {"type": "shape", "element_id": "b", "text": "Title #1"},
    ]
    report = {
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "section_titles": {
                        "max_chars_per_section_title": 20,
                        "max_chars_per_description": 6,
                    }
                }
            }
        }
    }
    s = evaluate._qbr_agenda_adapt_extra_rules(report, text_elements)
    assert "20" in s
    assert "6" in s
    assert "description" in s.lower()


def test_shorten_agenda_label_acronym():
    assert evaluate._shorten_agenda_label(
        "Quarterly Business Review", "acronym", None
    ) == "QBR"


def test_shorten_agenda_label_first_word_skips_article():
    assert evaluate._shorten_agenda_label(
        "The Financial Results", "first_word", None
    ) == "Financial"


def test_shorten_agenda_label_none_truncates():
    assert evaluate._shorten_agenda_label(
        "abcdefghijklmnopqrstuvwxyz", "none", 10
    ) == "abcdefghi…"


def test_build_qbr_agenda_reshorten_replacements():
    text_elements = [
        {"type": "shape", "element_id": "a", "text": "Agenda"},
        {"type": "shape", "element_id": "b", "text": "Long Section Name Here"},
    ]
    report = {
        "_slide_plan": [
            {"slide_type": "qbr_divider", "title": "Long Section Name Here"},
        ],
        "_hydrate_slide_hints": {
            "qbr_agenda": {
                "template": {
                    "section_titles": {
                        "from_deck_plan": True,
                        "label_shortening": {"mode": "acronym"},
                    },
                    "slide_detection": {"body_contains_word": ["Agenda"]},
                }
            }
        },
    }
    rows = evaluate._build_qbr_agenda_reshorten_replacements(text_elements, report)
    assert len(rows) == 1
    assert rows[0]["original"] == "Long Section Name Here"
    assert rows[0]["new_value"] == "LSNH"


# ── intake: Drive query escape ──────────────────────────────────────────────────


def test_drive_query_escape_apostrophe():
    assert evaluate._drive_query_escape("a'b") == "a\\'b"
