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
    assert "Synonym:" in notes
    assert "total sites" in notes
    assert "`total_sites`" in notes
