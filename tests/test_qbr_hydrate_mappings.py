"""Tests for explicit QBR hydrate mapping (config/qbr_mappings.yaml)."""

from __future__ import annotations

import pytest
import yaml

from src.qbr_hydrate_mappings import (
    apply_explicit_qbr_mappings,
    bootstrap_qbr_mappings_from_slides,
    build_adapt_page_slide_type_by_page_id,
    expand_mapping_rules,
    expand_qbr_mapping_source_candidates,
    mapping_source_is_recognizable_data,
    mapping_source_is_visual_only,
    mapping_source_suitable_for_qbr_yaml_autowrite,
    merge_discovered_sources_into_qbr_mappings,
)


def test_build_adapt_page_slide_type_skips_structural_types():
    report = {
        "_slide_plan": [
            {"slide_type": "title"},
            {"slide_type": "health"},
            {"slide_type": "qbr_cover"},
            {"slide_type": "qbr_divider"},
            {"slide_type": "qbr_agenda"},
        ]
    }
    out = build_adapt_page_slide_type_by_page_id(report, ["a", "b"])
    assert out["a"] == "health"
    assert out["b"] == "qbr_agenda"


def test_apply_explicit_bracket_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.qbr_hydrate_mappings.load_qbr_mappings",
        lambda **_: {
            "version": 1,
            "mappings": [
                {"slide_id": None, "source": "[000]", "target": "total_users"},
            ],
            "bracket_placeholder_sources": [],
        },
    )
    data_summary = {"total_users": 42}
    text_elements: list[dict] = []
    repls = [
        {
            "original": "[000]",
            "new_value": "[000]",
            "mapped": False,
            "field": "",
        }
    ]
    out = apply_explicit_qbr_mappings(
        repls,
        text_elements,
        data_summary,
        slide_type="health",
        slide_ref="1",
        slide_number=1,
    )
    assert out[0]["mapped"] is True
    assert out[0]["field"] == "total_users"
    assert "42" in str(out[0]["new_value"])


def test_apply_explicit_resolves_human_target_via_alias_map(monkeypatch: pytest.MonkeyPatch) -> None:
    """``target`` can be a human label listed in data_summary_target_aliases.json."""
    monkeypatch.setattr(
        "src.qbr_hydrate_mappings.load_qbr_mappings",
        lambda **_: {
            "version": 1,
            "mappings": [
                {"slide_id": None, "source": "XX%", "target": "Shortage Reduction"},
            ],
            "bracket_placeholder_sources": [],
        },
    )
    data_summary = {"total_critical_shortages": 12}
    text_elements = [{"type": "shape", "text": "Critical shortage reduction (XX% vs prior quarter)"}]
    repls = [
        {
            "original": "XX%",
            "new_value": "[00%]",
            "mapped": False,
            "field": "",
        }
    ]
    out = apply_explicit_qbr_mappings(
        repls,
        text_elements,
        data_summary,
        slide_type="health",
        slide_ref="1",
        slide_number=1,
    )
    assert out[0]["mapped"] is True
    assert out[0]["field"] == "total_critical_shortages"
    assert out[0]["synonym_path"] == "total_critical_shortages"
    assert "12" in str(out[0]["new_value"])


def test_apply_explicit_skips_blank_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.qbr_hydrate_mappings.load_qbr_mappings",
        lambda **_: {
            "version": 1,
            "mappings": [{"slide_id": None, "source": "[000]", "target": ""}],
            "bracket_placeholder_sources": [],
        },
    )
    repls = [{"original": "[000]", "new_value": "[000]", "mapped": False, "field": ""}]
    out = apply_explicit_qbr_mappings(
        repls, [], {"total_users": 1}, slide_type=None, slide_ref="1", slide_number=1
    )
    assert out[0]["mapped"] is False


def test_apply_explicit_slide_id_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.qbr_hydrate_mappings.load_qbr_mappings",
        lambda **_: {
            "version": 1,
            "mappings": [
                {"slide_id": "qbr_agenda", "source": "[000]", "target": "total_users"},
            ],
            "bracket_placeholder_sources": [],
        },
    )
    repls = [{"original": "[000]", "new_value": "[000]", "mapped": False, "field": ""}]
    out_wrong = apply_explicit_qbr_mappings(
        repls, [], {"total_users": 9}, slide_type="health", slide_ref="1"
    )
    assert out_wrong[0]["mapped"] is False
    out_ok = apply_explicit_qbr_mappings(
        repls, [], {"total_users": 9}, slide_type="qbr_agenda", slide_ref="2", slide_number=2
    )
    assert out_ok[0]["mapped"] is True


def test_expand_mapping_rules_v2_slide_and_global() -> None:
    cfg = {
        "version": 2,
        "slides": [
            {
                "slide_number": 4,
                "slide_id": "health",
                "elements": [
                    {"name": "sites_count", "source": "[000]", "target": "total_sites"},
                ],
            }
        ],
        "global_elements": [
            {"name": "any_slide_pct", "source": "[00%]", "target": "health_score"},
        ],
    }
    rows = expand_mapping_rules(cfg)
    assert len(rows) == 2
    by_name = {r["data_element_name"]: r for r in rows}
    assert by_name["sites_count"]["slide_number"] == 4
    assert by_name["sites_count"]["slide_id"] == "health"
    assert by_name["any_slide_pct"]["slide_number"] is None


def test_apply_explicit_respects_slide_number(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.qbr_hydrate_mappings.load_qbr_mappings",
        lambda **_: {
            "version": 2,
            "slides": [
                {
                    "slide_number": 99,
                    "elements": [
                        {"name": "only_99", "source": "[000]", "target": "total_users"},
                    ],
                }
            ],
            "global_elements": [],
        },
    )
    repls = [{"original": "[000]", "new_value": "[000]", "mapped": False, "field": ""}]
    out_wrong = apply_explicit_qbr_mappings(
        repls, [], {"total_users": 1}, slide_type="health", slide_ref="1", slide_number=1
    )
    assert out_wrong[0]["mapped"] is False
    out_ok = apply_explicit_qbr_mappings(
        repls, [], {"total_users": 1}, slide_type="health", slide_ref="99", slide_number=99
    )
    assert out_ok[0]["mapped"] is True
    assert out_ok[0].get("qbr_mapping_element") == "only_99"


def test_merge_appends_unmapped_sources(tmp_path) -> None:
    p = tmp_path / "qbr_mappings.yaml"
    p.write_text(
        yaml.dump(
            {
                "version": 2,
                "slides": [],
                "global_elements": [{"name": "keep", "source": "[000]", "target": ""}],
            },
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    n = merge_discovered_sources_into_qbr_mappings(
        [
            {"slide_number": 3, "slide_id": "health", "source": "Unique metric text"},
            {"slide_number": 3, "slide_id": "health", "source": "Unique metric text"},
        ],
        path=p,
    )
    assert n == 1
    cfg = yaml.safe_load(p.read_text(encoding="utf-8"))
    blocks = [b for b in (cfg.get("slides") or []) if isinstance(b, dict)]
    assert any(b.get("slide_number") == 3 for b in blocks)
    el = next(
        e
        for b in blocks
        if b.get("slide_number") == 3
        for e in (b.get("elements") or [])
        if isinstance(e, dict) and e.get("source") == "Unique metric text"
    )
    assert el.get("target") == ""
    assert str(el.get("name", "")).startswith("auto_s3_")


def test_mapping_source_is_recognizable_data() -> None:
    assert mapping_source_is_recognizable_data("[000]")
    assert mapping_source_is_recognizable_data("[???]")
    assert mapping_source_is_recognizable_data("42%")
    assert mapping_source_is_recognizable_data("$1.2M")
    assert mapping_source_is_recognizable_data("Jan 2025")
    assert mapping_source_is_recognizable_data("01/15/2025")
    assert mapping_source_is_recognizable_data("Unique metric phrase")
    assert not mapping_source_is_recognizable_data("Key metrics")
    assert not mapping_source_is_recognizable_data("Section title")
    assert not mapping_source_is_recognizable_data("")


def test_mapping_source_suitable_for_qbr_yaml_autowrite() -> None:
    assert mapping_source_suitable_for_qbr_yaml_autowrite("XX%")
    assert mapping_source_suitable_for_qbr_yaml_autowrite("Unique metric text")
    long_two_para = (
        "Para one with padding padding padding padding padding padding padding.\n\n"
        "Para two with XX% and more padding to exceed one hundred characters in this autowrite probe."
    )
    assert len(long_two_para) > 100
    assert not mapping_source_suitable_for_qbr_yaml_autowrite(long_two_para)
    assert not mapping_source_suitable_for_qbr_yaml_autowrite("x" * 501)


def test_expand_qbr_mapping_source_candidates_two_placeholder_lines() -> None:
    src = (
        "xx% DOI (Backwards) improvement\n"
        "yy% Clear to Build improvement (ideally we know resulting COTD improvement as well from our champion)"
    )
    parts = expand_qbr_mapping_source_candidates(src)
    assert len(parts) == 2
    assert parts[0].startswith("xx%")
    assert parts[1].startswith("yy%")


def test_expand_qbr_mapping_source_candidates_metric_plus_prose_one_row() -> None:
    """Do not split when a line is coaching copy, not its own metric row."""
    src = "xx% DOI (Backwards) improvement\nTry the feedback sandwich with more coaching copy on this line."
    parts = expand_qbr_mapping_source_candidates(src)
    assert len(parts) == 1
    assert "feedback sandwich" in parts[0]


def test_merge_splits_two_line_placeholder_shape(tmp_path) -> None:
    p = tmp_path / "qbr_mappings.yaml"
    p.write_text("version: 2\nslides: []\nglobal_elements: []\n", encoding="utf-8")
    src = "xx% line one metric\nyy% line two metric"
    n = merge_discovered_sources_into_qbr_mappings(
        [{"slide_number": 6, "slide_id": "pendo_localization", "source": src}],
        path=p,
    )
    assert n == 2
    cfg = yaml.safe_load(p.read_text(encoding="utf-8"))
    els = next(b["elements"] for b in cfg["slides"] if b.get("slide_number") == 6)
    sources = {e["source"] for e in els if isinstance(e, dict)}
    assert sources == {src.split("\n")[0].strip(), src.split("\n")[1].strip()}


def test_merge_skips_long_multiline_prose(tmp_path) -> None:
    p = tmp_path / "qbr_mappings.yaml"
    p.write_text("version: 2\nslides: []\nglobal_elements: []\n", encoding="utf-8")
    prose = (
        "Intro line with enough padding to exceed the autowrite length threshold for prose.\n\n"
        "Second paragraph with 12% in it and more padding here to pass one hundred chars total."
    )
    n = merge_discovered_sources_into_qbr_mappings(
        [{"slide_number": 3, "slide_id": "qbr_deployment", "source": prose}],
        path=p,
    )
    assert n == 0


def test_merge_skips_non_recognizable_sources(tmp_path) -> None:
    p = tmp_path / "qbr_mappings.yaml"
    p.write_text("version: 2\nslides: []\nglobal_elements: []\n", encoding="utf-8")
    n = merge_discovered_sources_into_qbr_mappings(
        [{"slide_number": 2, "slide_id": None, "source": "Key metrics"}],
        path=p,
    )
    assert n == 0


def test_mapping_source_is_visual_only() -> None:
    assert mapping_source_is_visual_only("(embedded image)")
    assert mapping_source_is_visual_only("(image in shape)")
    assert mapping_source_is_visual_only("(embedded chart — contains data that cannot be auto-updated)")
    assert mapping_source_is_visual_only("[STATIC IMAGE — x")
    assert mapping_source_is_visual_only("x", field="image")
    assert not mapping_source_is_visual_only("Active users: 12")


def test_merge_skips_visual_sources(tmp_path) -> None:
    p = tmp_path / "qbr_mappings.yaml"
    p.write_text("version: 2\nslides: []\nglobal_elements: []\n", encoding="utf-8")
    n = merge_discovered_sources_into_qbr_mappings(
        [{"slide_number": 2, "slide_id": None, "source": "(embedded image)", "field": None}],
        path=p,
    )
    assert n == 0
    cfg = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert not (cfg.get("slides") or [])


def test_bootstrap_slide_walk_writes_when_yaml_missing(tmp_path) -> None:
    p = tmp_path / "qbr_mappings.yaml"
    assert not p.exists()
    slides_by_id = {
        "p1": {
            "objectId": "p1",
            "pageElements": [
                {
                    "objectId": "sh1",
                    "shape": {
                        "text": {
                            "textElements": [
                                {"textRun": {"content": "Revenue $1.2M\n"}},
                            ]
                        }
                    },
                }
            ],
        }
    }
    n = bootstrap_qbr_mappings_from_slides(
        slides_by_id,
        ["p1"],
        ["p1"],
        {"p1": "health"},
        path=p,
    )
    assert n >= 1
    assert p.exists()
    assert bootstrap_qbr_mappings_from_slides(
        slides_by_id,
        ["p1"],
        ["p1"],
        {"p1": "health"},
        path=p,
    ) == 0


def test_merge_skips_existing_source_on_same_slide(tmp_path) -> None:
    p = tmp_path / "qbr_mappings.yaml"
    p.write_text(
        yaml.dump(
            {
                "version": 2,
                "slides": [
                    {
                        "slide_number": 2,
                        "elements": [{"name": "x", "source": "already", "target": "total_users"}],
                    }
                ],
                "global_elements": [],
            },
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    n = merge_discovered_sources_into_qbr_mappings(
        [{"slide_number": 2, "slide_id": None, "source": "already"}],
        path=p,
    )
    assert n == 0
