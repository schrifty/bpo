"""Tests for QBR template yellow/orange hint extraction."""
from unittest.mock import MagicMock, patch

from src import qbr_adapt_hints as hints


def _flat_hint_batches(batches: list[list[dict]]) -> list[dict]:
    return [r for b in batches for r in b]


def test_max_exclusive_index_trailing_paragraph_matches_slides_delete_bounds():
    """Slides text length often excludes the terminal paragraph marker (walk 14 vs usable 13)."""
    text_body = {
        "textElements": [
            {"startIndex": 0, "textRun": {"content": "a" * 13, "style": {}}},
            {"startIndex": 13, "paragraphMarker": {"style": {}}},
        ],
    }
    assert hints._text_body_walk_end_exclusive(text_body) == 14
    assert hints._text_body_max_exclusive_index(text_body) == 13


def test_hard_max_uses_api_end_index_when_smaller_than_walk():
    """``deleteText`` length follows Slides JSON ``endIndex`` — cap must not exceed it."""
    text_body = {
        "textElements": [
            {"startIndex": 0, "endIndex": 1, "textRun": {"content": "x", "style": {}}},
        ],
    }
    assert hints._api_max_end_index(text_body) == 1
    assert hints._hard_max_exclusive_index(text_body) == 1


def test_resolve_replace_delete_range_matches_source_when_hint_indices_invalid():
    """When merged/hint UTF-16 range is past API length, match ``source_text`` to the textRun."""
    m = hints._HintMutation(
        "s1",
        None,
        "replace",
        2,
        4,
        "[???]",
        source_text="Hi",
    )
    text_body = {
        "textElements": [
            {"startIndex": 0, "endIndex": 2, "textRun": {"content": "Hi", "style": {}}},
        ],
    }
    cl = hints._resolve_replace_delete_range(text_body, m)
    assert cl == (0, 2)


def test_max_exclusive_index_no_trailing_pm_still_caps_at_run_tail():
    """Slides may omit a terminal paragraphMarker but still report endIndex == walk (14) while delete max is 13."""
    text_body = {
        "textElements": [
            {"startIndex": 0, "endIndex": 14, "textRun": {"content": "a" * 13, "style": {}}},
        ],
    }
    assert hints._text_body_walk_end_exclusive(text_body) == 13
    assert hints._text_body_max_exclusive_index(text_body) == 13


def test_is_yellow_foreground():
    assert hints.is_yellow_foreground((1.0, 1.0, 0.0)) is True
    assert hints.is_yellow_foreground((0.9, 0.85, 0.2)) is True
    # Default Slides highlighter — high blue channel, previously missed by b<=0.55 rule
    assert hints.is_yellow_foreground((0.99, 0.99, 0.62)) is True
    assert hints.is_yellow_foreground((0.2, 0.2, 0.9)) is False


def test_is_orange_fill():
    assert hints.is_orange_fill((1.0, 0.5, 0.1)) is True
    assert hints.is_orange_fill((0.9, 0.45, 0.15)) is True
    assert hints.is_orange_fill((1.0, 1.0, 1.0)) is False


def test_extract_yellow_from_highlight_background_not_foreground():
    """Fill-in fields often use yellow **highlight** with dark body text (API omits yellow on foreground)."""
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "CSM",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 0.15, "green": 0.15, "blue": 0.2}},
                                },
                                "backgroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    out = hints.extract_template_adapt_hints_from_slide(slide)
    assert any("CSM" in s for s in out["yellow_segments"])


def test_extract_orange_foreground_caption():
    slide = {
        "pageElements": [{
            "objectId": "cap1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "We will record the review.",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 0.95, "green": 0.45, "blue": 0.1}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    out = hints.extract_template_adapt_hints_from_slide(slide)
    assert any("record" in s.lower() for s in out["orange_segments"])


def test_shape_orange_fill_detected_via_theme_when_no_rgb():
    slide = {
        "pageElements": [{
            "objectId": "th1",
            "shape": {
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "propertyState": "RENDERED",
                        "solidFill": {"color": {"themeColor": "ACCENT2"}},
                    }
                },
                "text": {
                    "textElements": [
                        {"textRun": {"content": "Headlines", "style": {}}},
                    ]
                },
            },
        }],
    }
    out = hints.extract_template_adapt_hints_from_slide(slide)
    assert out["orange_segments"]


def test_collect_mutations_orange_text_uses_delete_only():
    slide = {
        "pageElements": [{
            "objectId": "x1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"startIndex": 0, "textRun": {
                            "content": "Orange line",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 0.95, "green": 0.45, "blue": 0.1}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert any(m.replacement == "" for m in muts)


def test_orange_glyph_after_paragraph_marker_uses_indices_past_marker():
    """Regression (slide 29 batchUpdate): skipping paragraphMarker made the first textRun span (0,1)
    while Slides indices reserved 0 for the marker — deleteText endIndex exceeded document length.
    """
    orange = {"foregroundColor": {"opaqueColor": {"rgbColor": {"red": 0.95, "green": 0.45, "blue": 0.1}}}}
    text_body = {
        "textElements": [
            {"startIndex": 0, "paragraphMarker": {"style": {}}},
            {"textRun": {"content": "\u2b24", "style": orange}},
        ],
    }
    slide = {
        "pageElements": [{
            "objectId": "sym1",
            "shape": {"shapeProperties": {}, "text": text_body},
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert len(muts) == 1
    assert (muts[0].start, muts[0].end) == (1, 2)
    assert hints._text_body_max_exclusive_index(text_body) == 2
    batches, _ = hints.hint_mutations_to_batch_requests("pg", muts, add_banner=False, slide=slide)
    assert batches[0][0]["deleteText"]["textRange"]["startIndex"] == 1
    assert batches[0][0]["deleteText"]["textRange"]["endIndex"] == 2


@patch.object(hints, "_add_incomplete_banner", return_value=[])
def test_two_non_touching_yellow_spans_one_batch_prevents_index_drift(mock_banner):
    """Multiple replaces on the same shape must be one batchUpdate; sequential batches used stale indices."""
    yellow = {"foregroundColor": {"opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}}}}
    text_body = {
        "textElements": [
            {"textRun": {"content": "aa", "style": yellow}},
            {"textRun": {"content": "        ", "style": {}}},
            {"textRun": {"content": "bb", "style": yellow}},
        ],
    }
    slide = {
        "pageElements": [{
            "objectId": "p1",
            "shape": {"shapeProperties": {}, "text": text_body},
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert len(muts) == 2
    batches, _ = hints.hint_mutations_to_batch_requests("pg", muts, add_banner=False, slide=slide)
    flat = _flat_hint_batches(batches)
    # Prefer page-scoped replaceAllText when each span is unique on the slide (no UTF-16 deleteText).
    rat = [r for r in flat if "replaceAllText" in r]
    assert len(rat) == 2
    assert all("pg" in (r.get("replaceAllText") or {}).get("pageObjectIds", []) for r in rat)


@patch.object(hints, "_add_incomplete_banner", return_value=[])
def test_merge_touching_yellow_replaces_single_delete_span(mock_banner):
    """Overlapping/touching yellow runs must merge so batchUpdate delete indices stay valid."""
    yellow = {"foregroundColor": {"opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}}}}
    text_body = {
        "textElements": [
            {"textRun": {"content": "aaa", "style": yellow}},
            {"textRun": {"content": "bbb", "style": yellow}},
        ],
    }
    slide = {
        "pageElements": [{
            "objectId": "p1",
            "shape": {"shapeProperties": {}, "text": text_body},
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert len(muts) == 2
    batches, content_n = hints.hint_mutations_to_batch_requests("pg", muts, add_banner=False, slide=slide)
    flat = _flat_hint_batches(batches)
    rat = [r for r in flat if "replaceAllText" in r]
    assert len(rat) == 1
    assert rat[0]["replaceAllText"]["containsText"]["text"] == "aaabbb"


@patch.object(hints, "_add_incomplete_banner", return_value=[])
def test_hint_mutations_clamps_delete_when_api_endindex_exceeds_walk(mock_banner):
    """Regression: Slides batchUpdate rejects deleteText when endIndex > document length."""
    thirteen = "a" * 13
    text_body = {
        "textElements": [
            {"startIndex": 0, "endIndex": 14, "textRun": {"content": thirteen, "style": {}}},
        ],
    }
    slide = {
        "pageElements": [{
            "objectId": "p1",
            "shape": {"shapeProperties": {}, "text": text_body},
        }],
    }
    muts = [hints._HintMutation("p1", None, "replace", 0, 14, hints.YELLOW_FIELD_PLACEHOLDER)]
    batches, _ = hints.hint_mutations_to_batch_requests("pg", muts, add_banner=False, slide=slide)
    assert batches[0][0]["deleteText"]["textRange"]["endIndex"] == 13
    assert batches[0][0]["deleteText"]["textRange"]["startIndex"] == 0


def test_extract_yellow_from_shape_runs():
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {"content": "Normal ", "style": {}}},
                        {
                            "textRun": {
                                "content": "42",
                                "style": {
                                    "foregroundColor": {
                                        "opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}}
                                    }
                                },
                            }
                        },
                        {"textRun": {"content": " sites", "style": {}}},
                    ]
                },
            },
        }],
    }
    out = hints.extract_template_adapt_hints_from_slide(slide)
    assert any("42" in s for s in out["yellow_segments"])


def test_extract_orange_box_empty_shape_yields_marker():
    slide = {
        "pageElements": [{
            "objectId": "o0",
            "shape": {
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "propertyState": "RENDERED",
                        "solidFill": {"color": {"rgbColor": {"red": 0.95, "green": 0.5, "blue": 0.1}}},
                    }
                },
                "text": {},
            },
        }],
    }
    out = hints.extract_template_adapt_hints_from_slide(slide)
    assert hints._ORANGE_SHAPE_EMPTY_MARKER in (out["orange_segments"] or [])


def test_extract_orange_box_full_text():
    slide = {
        "pageElements": [{
            "objectId": "s2",
            "shape": {
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "propertyState": "RENDERED",
                        "solidFill": {"color": {"rgbColor": {"red": 0.95, "green": 0.5, "blue": 0.1}}},
                    }
                },
                "text": {
                    "textElements": [
                        {"textRun": {"content": "Refresh only KPIs above.", "style": {}}},
                    ]
                },
            },
        }],
    }
    out = hints.extract_template_adapt_hints_from_slide(slide)
    assert out["orange_segments"]
    assert "KPIs" in out["orange_segments"][0]


def test_build_hint_rows_slide_order():
    final = [
        {"objectId": "a", "pageElements": []},
        {"objectId": "b", "pageElements": []},
    ]
    rows = hints.build_hint_rows_for_adapt_slides(final, ["b"])
    assert len(rows) == 1
    assert rows[0]["slide_num"] == 2


def test_thin_and_figure_space_only_yellow_runs_skipped():
    """U+2009 thin space / U+2007 figure space: not stripped by plain .strip() alone."""
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "\u2009\u2007\u202f",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert not any(m.replacement == hints.YELLOW_FIELD_PLACEHOLDER for m in muts)


def test_nbsp_only_yellow_run_skipped():
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "\u00a0\u00a0",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert not any(m.replacement == hints.YELLOW_FIELD_PLACEHOLDER for m in muts)


def test_merge_runs_by_yellow_drops_zwsp_only_segment():
    body = {
        "textElements": [
            {"textRun": {
                "content": "\u200b\u200b",
                "style": {
                    "foregroundColor": {"opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}}},
                },
            }},
        ],
    }
    y, _o = hints._merge_runs_by_yellow(body)
    assert y == []


def test_yellow_styled_whitespace_run_does_not_get_placeholder_or_banner_claim():
    """Slides often carry yellow/highlight styling on newline-only runs — no visible [???]."""
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "\n \n",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert not any(m.replacement == hints.YELLOW_FIELD_PLACEHOLDER for m in muts)
    assert "[???]" not in hints.qbr_hint_banner_text_for_mutations(muts)


def test_collect_hint_mutations_yellow_run():
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {
                            "startIndex": 0,
                            "textRun": {
                                "content": "x",
                                "style": {
                                    "foregroundColor": {
                                        "opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}}
                                    }
                                },
                            },
                        },
                    ]
                },
            },
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert len(muts) == 1
    assert muts[0].action == "replace"
    assert muts[0].start == 0
    assert muts[0].end == 1
    assert muts[0].replacement == hints.YELLOW_FIELD_PLACEHOLDER


def test_collect_hint_mutations_orange_shape_is_deleted():
    slide = {
        "pageElements": [{
            "objectId": "o1",
            "shape": {
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "propertyState": "RENDERED",
                        "solidFill": {"color": {"rgbColor": {"red": 0.95, "green": 0.5, "blue": 0.1}}},
                    }
                },
                "text": {
                    "textElements": [
                        {"textRun": {"content": "Note", "style": {}}},
                    ]
                },
            },
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert len(muts) == 1
    assert muts[0].action == "delete_shape"
    assert muts[0].cell_location is None


def test_collect_hint_mutations_orange_shape_no_text_still_deleted():
    slide = {
        "pageElements": [{
            "objectId": "o_empty",
            "shape": {
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "propertyState": "RENDERED",
                        "solidFill": {"color": {"rgbColor": {"red": 0.95, "green": 0.5, "blue": 0.1}}},
                    }
                },
                "text": {},
            },
        }],
    }
    muts = hints.collect_hint_mutations_from_slide(slide)
    assert len(muts) == 1
    assert muts[0].action == "delete_shape"


@patch.object(hints, "_add_incomplete_banner", return_value=[])
def test_hint_mutations_orange_text_delete_only_no_insert(mock_banner):
    muts = [hints._HintMutation("p1", None, "replace", 0, 5, "")]
    slide = {
        "pageElements": [{
            "objectId": "p1",
            "shape": {
                "shapeProperties": {},
                "text": {"textElements": [{"textRun": {"content": "hello", "style": {}}}]},
            },
        }],
    }
    batches, content_n = hints.hint_mutations_to_batch_requests(
        "pageZ", muts, add_banner=False, slide=slide
    )
    assert content_n == 1
    assert len(batches) == 1 and len(batches[0]) == 1
    assert "deleteText" in batches[0][0]


@patch.object(hints, "_add_incomplete_banner", return_value=[{"should_not": "appear"}])
def test_hint_mutations_respects_add_banner_false(mock_banner):
    muts = [hints._HintMutation("box1", None, "delete_shape")]
    batches, _ = hints.hint_mutations_to_batch_requests("pageZ", muts, add_banner=False)
    assert batches == [[{"deleteObject": {"objectId": "box1"}}]]
    mock_banner.assert_not_called()


@patch.object(hints, "_add_incomplete_banner", return_value=[])
def test_hint_mutations_delete_shape_emits_delete_object(mock_banner):
    muts = [hints._HintMutation("box1", None, "delete_shape")]
    batches, content_n = hints.hint_mutations_to_batch_requests("pageZ", muts)
    assert content_n == 1
    assert batches == [[{"deleteObject": {"objectId": "box1"}}]]


@patch.object(hints, "_add_incomplete_banner", return_value=[{"banner": True}])
def test_hint_mutations_to_batch_requests(mock_banner):
    muts = [
        hints._HintMutation("p1", None, "replace", 2, 5, hints.YELLOW_FIELD_PLACEHOLDER),
    ]
    slide = {
        "pageElements": [{
            "objectId": "p1",
            "shape": {
                "shapeProperties": {},
                "text": {"textElements": [{"textRun": {"content": "xxxxx", "style": {}}}]},
            },
        }],
    }
    batches, content_n = hints.hint_mutations_to_batch_requests("pageZ", muts, slide=slide)
    assert content_n == 3
    flat = _flat_hint_batches(batches)
    assert flat[-1] == {"banner": True}
    assert any(
        r.get("deleteText", {}).get("textRange", {}).get("startIndex") == 2
        for r in flat
    )
    assert any(
        r.get("insertText", {}).get("text") == hints.YELLOW_FIELD_PLACEHOLDER
        for r in flat
    )
    assert any(r.get("updateTextStyle") for r in flat[:-1])
    mock_banner.assert_called_once()
    _args, kwargs = mock_banner.call_args
    assert _args[0] == "pageZ"
    assert kwargs.get("banner_text") == hints.qbr_hint_banner_text_for_mutations(muts)


def test_qbr_hint_banner_text_orange_only():
    muts = [hints._HintMutation("b", None, "delete_shape")]
    t = hints.qbr_hint_banner_text_for_mutations(muts)
    assert "[???]" not in t
    assert "orange" in t.lower()


def test_qbr_hint_banner_text_yellow_only():
    muts = [hints._HintMutation("p", None, "replace", 0, 1, hints.YELLOW_FIELD_PLACEHOLDER)]
    t = hints.qbr_hint_banner_text_for_mutations(muts)
    assert "[???]" in t
    assert "orange" not in t.lower()


def test_qbr_hint_banner_text_both():
    muts = [
        hints._HintMutation("p", None, "replace", 0, 1, hints.YELLOW_FIELD_PLACEHOLDER),
        hints._HintMutation("b", None, "delete_shape"),
    ]
    t = hints.qbr_hint_banner_text_for_mutations(muts)
    assert "[???]" in t and "orange" in t.lower()


def test_post_adapt_strips_any_opaque_text_highlight():
    """replaceAllText can leave non-yellow highlights; post-adapt clears any run background."""
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "Done",
                            "style": {
                                "backgroundColor": {
                                    "opaqueColor": {"themeColor": "ACCENT1"},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    reqs = hints.build_post_adapt_template_style_strip_requests(slide)
    assert len(reqs) == 1
    assert reqs[0]["updateTextStyle"]["style"]["backgroundColor"] == {}


def test_post_adapt_style_strip_clears_yellow_highlight_without_changing_detection():
    """replaceAllText keeps highlight — post-adapt pass issues updateTextStyle only."""
    slide = {
        "pageElements": [{
            "objectId": "s1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "Acme Corp",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 0.15, "green": 0.15, "blue": 0.2}},
                                },
                                "backgroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 1, "green": 1, "blue": 0}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    reqs = hints.build_post_adapt_template_style_strip_requests(slide)
    assert len(reqs) == 1
    uts = reqs[0]["updateTextStyle"]
    assert uts["objectId"] == "s1"
    assert uts["textRange"]["startIndex"] == 0
    assert uts["style"]["backgroundColor"] == {}


@patch.object(hints, "persist_qbr_template_authoring_cues", return_value=None)
@patch.object(hints, "apply_hint_mutations_to_presentation", return_value=0)
@patch.object(hints, "analyze_adapt_hints_with_llm")
def test_run_qbr_adapt_hints_phase_always_runs_surface_cleanup(mock_llm, mock_apply, _mock_persist):
    """Regression: empty extraction used to return early and skip apply_hint_mutations entirely."""
    hints.run_qbr_adapt_hints_phase(
        MagicMock(),
        MagicMock(),
        "pres-1",
        [{"objectId": "z1", "pageElements": []}],
        ["z1"],
        "Acme",
    )
    mock_llm.assert_not_called()
    mock_apply.assert_called_once()
    rows_arg = mock_apply.call_args[0][2]
    assert len(rows_arg) == 1
    assert rows_arg[0]["object_id"] == "z1"


@patch.object(hints, "persist_qbr_template_authoring_cues", return_value=None)
@patch.object(hints, "apply_hint_mutations_to_presentation", return_value=1)
@patch.object(hints, "analyze_adapt_hints_with_llm", return_value={"slides": [], "overall_useful": True, "overall_summary": "ok"})
def test_run_qbr_adapt_hints_phase_llm_only_when_segments(mock_llm, mock_apply, _mock_persist):
    slide = {
        "objectId": "s1",
        "pageElements": [{
            "objectId": "cap1",
            "shape": {
                "shapeProperties": {},
                "text": {
                    "textElements": [
                        {"textRun": {
                            "content": "tip",
                            "style": {
                                "foregroundColor": {
                                    "opaqueColor": {"rgbColor": {"red": 0.95, "green": 0.45, "blue": 0.1}},
                                },
                            },
                        }},
                    ]
                },
            },
        }],
    }
    hints.run_qbr_adapt_hints_phase(
        MagicMock(),
        MagicMock(),
        "pres-2",
        [slide],
        ["s1"],
        "Acme",
    )
    mock_llm.assert_called_once()
    mock_apply.assert_called_once()
    assert len(mock_apply.call_args[0][2]) == 1


def test_persist_qbr_template_authoring_cues_writes_file(tmp_path, monkeypatch):
    target = tmp_path / "qbr-template-authoring-cues.yaml"
    monkeypatch.setattr(hints, "_qbr_authoring_cues_yaml_path", lambda: target)
    out = hints.persist_qbr_template_authoring_cues(
        [
            {
                "slide_num": 2,
                "object_id": "oid1",
                "title_guess": "Agenda",
                "yellow_segments": ["x"],
                "orange_segments": [],
            }
        ],
        {"overall_useful": True, "overall_summary": "ok", "slides": []},
        customer="Acme",
        manifest_sha16="deadbeef",
    )
    assert out == str(target)
    text = target.read_text(encoding="utf-8")
    assert "oid1" in text
    assert "deadbeef" in text
    assert "Agenda" in text
