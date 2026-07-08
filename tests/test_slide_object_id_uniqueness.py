"""Regression tests for globally-unique slide page object IDs.

Google Slides ``batchUpdate`` fails with a 400 ("The object ID ... should be unique among all
pages and page elements") if two ``createSlide`` requests share an objectId. This happened in the
nightly ``engineering-portfolio`` deck (``s_ai_productivity_matrix_42`` emitted twice). The renderer
now guarantees unique base object IDs; every child element ID is derived from the base, so a unique
base keeps the whole slide unique.
"""

from __future__ import annotations

from src import deck_renderer
from src.slide_utils import slide_object_id_base, unique_slide_object_id_base


def test_unique_slide_object_id_base_returns_base_when_free():
    used: set[str] = set()
    sid = unique_slide_object_id_base("ai_productivity_matrix", 42, used)
    assert sid == slide_object_id_base("ai_productivity_matrix", 42) == "s_ai_productivity_matrix_42"


def test_unique_slide_object_id_base_salts_on_collision():
    base = slide_object_id_base("ai_productivity_matrix", 42)
    used = {base}
    sid = unique_slide_object_id_base("ai_productivity_matrix", 42, used)
    assert sid != base
    assert sid not in used


def test_unique_slide_object_id_base_does_not_mutate_used():
    used = {slide_object_id_base("x", 1)}
    before = set(used)
    unique_slide_object_id_base("x", 1, used)
    assert used == before


def _stalling_builder(reqs, sid, report, idx):
    """Fake builder that creates a slide but does NOT advance idx (forces a seq collision)."""
    reqs.append({"createSlide": {"objectId": sid}})
    return idx


def test_render_slide_plan_deduplicates_colliding_page_ids(monkeypatch):
    # Two entries share the same id; the stalling builder keeps idx fixed so both would resolve
    # to the same base object ID without the guard.
    monkeypatch.setattr(deck_renderer, "get_slide_builder", lambda slide_type: _stalling_builder)
    plan = [
        {"id": "ai_productivity_matrix", "slide_type": "ai_productivity_matrix"},
        {"id": "ai_productivity_matrix", "slide_type": "ai_productivity_matrix"},
    ]
    reqs, _created, _notes, _deferred, _work = deck_renderer.render_slide_plan({}, plan, "engineering-portfolio")

    create_ids = [r["createSlide"]["objectId"] for r in reqs if "createSlide" in r]
    assert len(create_ids) == 2
    assert len(set(create_ids)) == 2, f"duplicate page object IDs emitted: {create_ids}"
