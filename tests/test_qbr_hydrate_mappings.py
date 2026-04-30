"""Tests for explicit QBR hydrate mapping (config/qbr_mappings.yaml)."""

from __future__ import annotations

import pytest

from src.qbr_hydrate_mappings import (
    apply_explicit_qbr_mappings,
    build_adapt_page_slide_type_by_page_id,
    expand_mapping_rules,
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
