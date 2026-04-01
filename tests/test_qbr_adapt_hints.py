"""Tests for QBR template yellow/orange hint extraction."""
from unittest.mock import patch

from src import qbr_adapt_hints as hints


def test_is_yellow_foreground():
    assert hints.is_yellow_foreground((1.0, 1.0, 0.0)) is True
    assert hints.is_yellow_foreground((0.9, 0.85, 0.2)) is True
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
    reqs, _ = hints.hint_mutations_to_batch_requests("pg", muts, add_banner=False, slide=slide)
    assert reqs[0]["deleteText"]["textRange"]["startIndex"] == 1
    assert reqs[0]["deleteText"]["textRange"]["endIndex"] == 2


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
    reqs, _ = hints.hint_mutations_to_batch_requests("pg", muts, add_banner=False, slide=slide)
    assert reqs[0]["deleteText"]["textRange"]["endIndex"] == 13
    assert reqs[0]["deleteText"]["textRange"]["startIndex"] == 0


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
    reqs, content_n = hints.hint_mutations_to_batch_requests("pageZ", muts)
    assert content_n == 1
    assert len(reqs) == 1
    assert "deleteText" in reqs[0]


@patch.object(hints, "_add_incomplete_banner", return_value=[{"should_not": "appear"}])
def test_hint_mutations_respects_add_banner_false(mock_banner):
    muts = [hints._HintMutation("box1", None, "delete_shape")]
    reqs, _ = hints.hint_mutations_to_batch_requests("pageZ", muts, add_banner=False)
    assert reqs == [{"deleteObject": {"objectId": "box1"}}]
    mock_banner.assert_not_called()


@patch.object(hints, "_add_incomplete_banner", return_value=[])
def test_hint_mutations_delete_shape_emits_delete_object(mock_banner):
    muts = [hints._HintMutation("box1", None, "delete_shape")]
    reqs, content_n = hints.hint_mutations_to_batch_requests("pageZ", muts)
    assert content_n == 1
    assert reqs == [{"deleteObject": {"objectId": "box1"}}]


@patch.object(hints, "_add_incomplete_banner", return_value=[{"banner": True}])
def test_hint_mutations_to_batch_requests(mock_banner):
    muts = [
        hints._HintMutation("p1", None, "replace", 2, 5, hints.YELLOW_FIELD_PLACEHOLDER),
    ]
    reqs, content_n = hints.hint_mutations_to_batch_requests("pageZ", muts)
    assert content_n == 2
    assert reqs[-1] == {"banner": True}
    assert any(
        r.get("deleteText", {}).get("textRange", {}).get("startIndex") == 2
        for r in reqs
    )
    assert any(
        r.get("insertText", {}).get("text") == hints.YELLOW_FIELD_PLACEHOLDER
        for r in reqs
    )
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
