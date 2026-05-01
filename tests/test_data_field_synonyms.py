"""Tests for config/data_field_synonyms.json resolution."""
from pathlib import Path

from src import data_field_synonyms as dfs
from src import evaluate


def test_data_summary_lookup_nested():
    ds = {"platform_value": {"total_savings": 42}}
    assert dfs.data_summary_lookup(ds, "platform_value.total_savings") == 42
    assert dfs.data_summary_lookup(ds, "platform_value.missing") is None


def test_try_resolve_phrase_cost_avoidance():
    hay = "Headline: cost avoidance on inventory for QBR"
    ds = {"platform_value": {"total_savings": 1_234_567}}
    hit = dfs.try_resolve_phrase_in_text(hay, ds)
    assert hit is not None
    phrase, path, _disp, raw = hit
    assert "cost avoidance" in phrase.lower() or phrase
    assert path == "platform_value.total_savings"
    assert raw == 1_234_567


def test_synonym_skips_absurd_value_when_slide_is_percent_context(monkeypatch):
    """Do not inject hours/minutes-scale scalars into a % slot (e.g. 39371 for 91%)."""
    def fake_hit(*_a, **_kw):
        return ("fake", "account_avg_weekly_hours", "account_avg_weekly_hours", 39371.5)

    monkeypatch.setattr(dfs, "try_resolve_phrase_in_text", fake_hit)
    text_elements = [{"type": "shape", "text": "91% of the COGS under management"}]
    repl = [
        {
            "original": "91%",
            "new_value": "[000]",
            "mapped": False,
            "field": "?",
        }
    ]
    ds = {"account_avg_weekly_hours": 39371.5}
    out = dfs.apply_synonym_resolution_to_replacements(repl, text_elements, ds)
    assert len(out) == 1
    assert out[0]["mapped"] is False


def test_synonym_preserves_percent_sign_when_prefix_had_percent(monkeypatch):
    def fake_hit(*_a, **_kw):
        return ("fake", "weekly_active_buyers_pct_avg", "weekly_active_buyers_pct_avg", 42.3)

    monkeypatch.setattr(dfs, "try_resolve_phrase_in_text", fake_hit)
    text_elements = [{"type": "shape", "text": "91% of the COGS under management"}]
    repl = [
        {
            "original": "91% of the COGS under management",
            "new_value": "[000]",
            "mapped": False,
            "field": "?",
        }
    ]
    ds = {"weekly_active_buyers_pct_avg": 42.3}
    out = dfs.apply_synonym_resolution_to_replacements(repl, text_elements, ds)
    assert len(out) == 1
    assert out[0]["mapped"] is True
    assert "42.3%" in out[0]["new_value"]
    assert "of the COGS" in out[0]["new_value"]


def test_synonym_narrow_haystack_same_shape_other_line_no_cross_match():
    """Placeholder lines must not inherit phrase matches from unrelated lines in the same text box."""
    text_elements = [
        {
            "type": "shape",
            "text": (
                "weekly on leandna for engagement metrics\n"
                "[4 BU]\n"
                "[8 Differents ERP]"
            ),
        },
    ]
    ds = {"account_avg_weekly_hours": 39371.5}
    for orig in ("[4 BU]", "[8 Differents ERP]"):
        repl = [
            {
                "original": orig,
                "new_value": "[000]",
                "mapped": False,
                "field": "?",
            }
        ]
        out = dfs.apply_synonym_resolution_to_replacements(repl, text_elements, ds)
        assert len(out) == 1, orig
        assert out[0]["mapped"] is False, orig
        assert out[0].get("synonym_path") is None, orig


def test_apply_synonym_to_unmapped_replacement():
    text_elements = [
        {"type": "shape", "text": "Average hours spent weekly on LeanDNA: [000]"},
    ]
    repl = [
        {
            "original": "[000]",
            "new_value": "[000]",
            "mapped": False,
            "field": "unknown_metric",
        }
    ]
    ds = {"account_avg_weekly_hours": 12.5}
    out = dfs.apply_synonym_resolution_to_replacements(repl, text_elements, ds)
    assert len(out) == 1
    assert out[0]["mapped"] is True
    assert out[0]["field"] == "account_avg_weekly_hours"
    assert out[0]["synonym_phrase"]
    assert "12.5" in out[0]["new_value"]


def test_config_file_exists():
    assert (Path(__file__).resolve().parents[1] / "config" / "data_field_synonyms.json").is_file()


def test_resolve_data_summary_target_path_synonym_phrase():
    dfs.invalidate_target_path_alias_cache()
    assert dfs.resolve_data_summary_target_path("cost avoidance") == "platform_value.total_savings"
    assert dfs.resolve_data_summary_target_path("Platform_value.Total_Savings") == "platform_value.total_savings"


def test_resolve_data_summary_target_path_aliases_file():
    dfs.invalidate_target_path_alias_cache()
    assert dfs.resolve_data_summary_target_path("Shortage Reduction") == "total_critical_shortages"
    assert dfs.resolve_data_summary_target_path("critical shortage reduction") == "total_critical_shortages"


def test_resolve_data_summary_target_path_unknown_passthrough():
    dfs.invalidate_target_path_alias_cache()
    assert dfs.resolve_data_summary_target_path("total_users") == "total_users"
    assert dfs.resolve_data_summary_target_path("  Unknown Metric XYZ  ") == "Unknown Metric XYZ"


def test_speaker_notes_include_synonym_line():
    reps = [
        {
            "original": "99",
            "new_value": "100",
            "mapped": True,
            "field": "total_sites",
            "synonym_phrase": "total sites",
            "synonym_path": "total_sites",
        }
    ]
    notes = evaluate._build_hydrate_speaker_notes(reps, [{"type": "shape", "text": "99"}])
    assert "synonym:" in notes
    assert "total sites" in notes
    assert "`total_sites`" in notes
