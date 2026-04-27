"""Shared helpers for invoking slide builders."""

from __future__ import annotations

from typing import Any

from .slide_metadata import SLIDE_DATA_REQUIREMENTS
from .slide_pipeline_traces import (
    build_slide_jql_speaker_notes_for_entry as _build_slide_jql_speaker_notes_for_entry_impl,
)


def normalize_builder_return(ret: Any, default_slide_id: str) -> tuple[int, list[str]]:
    """Slide builders return ``next_idx`` or ``(next_idx, [page_object_id, ...])``."""
    if isinstance(ret, tuple) and len(ret) == 2 and isinstance(ret[1], list):
        ids = [str(x) for x in ret[1] if x]
        return int(ret[0]), (ids if ids else [default_slide_id])
    return int(ret), [default_slide_id]


_normalize_builder_return = normalize_builder_return


def build_slide_jql_speaker_notes_for_entry(report: dict[str, Any], entry: dict[str, Any]) -> str:
    """Build speaker notes for one slide-plan entry using the slide registries."""
    return _build_slide_jql_speaker_notes_for_entry_impl(
        report,
        entry,
        data_requirements=SLIDE_DATA_REQUIREMENTS,
    )


_build_slide_jql_speaker_notes = build_slide_jql_speaker_notes_for_entry
