"""Google Slides speaker-note read/write helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from googleapiclient.errors import HttpError

from .config import logger
from .slides_api import slides_presentations_batch_update

__all__ = [
    "build_slide_jql_speaker_notes",
    "collect_data_trace_entries",
    "collect_declared_data_trace_entries",
    "collect_jql_soql_trace_entries",
    "dedupe_data_trace_entries",
    "get_speaker_notes_object_id",
    "set_speaker_notes",
    "set_speaker_notes_batch",
]


def collect_jql_soql_trace_entries(obj: Any) -> list[dict[str, str]]:
    """Recursively collect Jira ``jql_queries`` and Salesforce ``soql_queries`` only."""
    if obj is None:
        return []
    if isinstance(obj, dict):
        entries: list[dict[str, str]] = []
        jql_raw = obj.get("jql_queries")
        if isinstance(jql_raw, list):
            for item in jql_raw:
                if isinstance(item, dict) and str(item.get("jql") or "").strip():
                    entries.append({
                        "description": str(item.get("description") or "Jira issue search").strip(),
                        "source": "Jira",
                        "query": str(item["jql"]).strip(),
                    })
                elif isinstance(item, str) and item.strip():
                    entries.append({
                        "description": "Jira issue search",
                        "source": "Jira",
                        "query": item.strip(),
                    })
        soql_raw = obj.get("soql_queries")
        if isinstance(soql_raw, list):
            for item in soql_raw:
                if isinstance(item, dict):
                    q = str(item.get("soql") or item.get("query") or "").strip()
                    if q:
                        entries.append({
                            "description": str(item.get("description") or "Salesforce query").strip(),
                            "source": "Salesforce",
                            "query": q,
                        })
                elif isinstance(item, str) and item.strip():
                    entries.append({
                        "description": "Salesforce query",
                        "source": "Salesforce",
                        "query": item.strip(),
                    })
        for val in obj.values():
            entries.extend(collect_jql_soql_trace_entries(val))
        return entries
    if isinstance(obj, list):
        return [e for item in obj for e in collect_jql_soql_trace_entries(item)]
    return []


def collect_declared_data_trace_entries(obj: Any) -> list[dict[str, str]]:
    """Recursively collect ``data_traces`` (declared pipeline notes, not JQL/SOQL)."""
    if obj is None:
        return []
    if isinstance(obj, dict):
        entries: list[dict[str, str]] = []
        dt_raw = obj.get("data_traces")
        if isinstance(dt_raw, list):
            for item in dt_raw:
                if not isinstance(item, dict):
                    continue
                desc = str(item.get("description") or "").strip()
                src = str(item.get("source") or "Report").strip()
                q = str(item.get("query") or item.get("trace") or "").strip()
                if desc and q:
                    entries.append({"description": desc, "source": src, "query": q})
        for val in obj.values():
            entries.extend(collect_declared_data_trace_entries(val))
        return entries
    if isinstance(obj, list):
        return [e for item in obj for e in collect_declared_data_trace_entries(item)]
    return []


def collect_data_trace_entries(obj: Any) -> list[dict[str, str]]:
    """All trace rows: Jira, Salesforce, and declared ``data_traces``."""
    return collect_jql_soql_trace_entries(obj) + collect_declared_data_trace_entries(obj)


def dedupe_data_trace_entries(entries: list[dict[str, str]]) -> list[dict[str, str]]:
    """Drop duplicate (source, query) pairs; keep first description."""
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for e in entries:
        src = (e.get("source") or "Unknown").strip()
        q = (e.get("query") or "").strip()
        if not q:
            continue
        key = (src.casefold(), q)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "description": (e.get("description") or "Data").strip(),
            "source": src,
            "query": q,
        })
    return out


def build_slide_jql_speaker_notes(
    report: dict[str, Any],
    entry: dict[str, Any],
    *,
    data_requirements: dict[str, list[str]],
    canonical_pipeline_traces: dict[str, Any],
) -> str:
    """Build speaker notes with trace rows for a slide.

    ``canonical_pipeline_traces`` contains slide-specific trace builders owned by the
    slide-rendering layer; this module owns the generic collection and formatting.
    """
    prev_sn_entry = report.get("_speaker_note_slide_entry")
    report["_speaker_note_slide_entry"] = entry
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        slide_type = entry.get("slide_type", entry.get("id", "slide"))
        slide_title = entry.get("title", slide_type.replace("_", " ").title())
        header = [
            ts,
            "",
            f"Slide: {slide_title}",
            f"Slide type: {slide_type}",
        ]

        required_keys = data_requirements.get(slide_type, [])
        canon_fn = canonical_pipeline_traces.get(slide_type)

        pipeline: list[dict[str, str]] = []
        if canon_fn is not None:
            pipeline = canon_fn(report)
        elif required_keys:
            for key in required_keys:
                pipeline.extend(collect_declared_data_trace_entries(report.get(key)))
            pipeline = dedupe_data_trace_entries(pipeline)
        else:
            pipeline = dedupe_data_trace_entries(collect_declared_data_trace_entries(report))

        executable: list[dict[str, str]] = []
        if required_keys:
            for key in required_keys:
                executable.extend(collect_jql_soql_trace_entries(report.get(key)))
        else:
            executable = collect_jql_soql_trace_entries(report)
        executable = dedupe_data_trace_entries(executable)

        entries = dedupe_data_trace_entries(pipeline + executable)

        if not entries:
            if slide_type in ("salesforce_comprehensive_cover", "salesforce_category"):
                header.append("")
                header.append(
                    "Live Salesforce metrics: Salesforce - SOQL via REST API (per-object queries not recorded in this payload)"
                )
            return "\n".join(header)

        header.append("")
        n = len(entries)
        for i, e in enumerate(entries):
            desc = (e.get("description") or "Data").strip()
            src = (e.get("source") or "Unknown").strip()
            q = (e.get("query") or "").strip()
            header.append(f"• {desc} — {src}")
            if q:
                for part in q.splitlines():
                    p = part.strip()
                    if p:
                        header.append(f"  {p}")
            if i < n - 1:
                header.append("")
        return "\n".join(header)
    finally:
        if prev_sn_entry is not None:
            report["_speaker_note_slide_entry"] = prev_sn_entry
        else:
            report.pop("_speaker_note_slide_entry", None)


def get_speaker_notes_object_id(slides_svc, pres_id: str, slide_page_id: str) -> str | None:
    """Return the object ID of the speaker-notes shape for the given slide, or None if not found.

    Uses slide's slideProperties.notesPage (embedded) or notesPageId + pages.get for
    notesProperties.speakerNotesObjectId.
    """
    fields = "slides(objectId,slideProperties(notesPage(objectId,notesProperties(speakerNotesObjectId))))"
    pres = slides_svc.presentations().get(
        presentationId=pres_id, fields=fields
    ).execute()
    for page in pres.get("slides", []):
        if page.get("objectId") != slide_page_id:
            continue
        sp = page.get("slideProperties") or {}
        notes_page = sp.get("notesPage")
        if isinstance(notes_page, dict):
            oid = (notes_page.get("notesProperties") or {}).get("speakerNotesObjectId")
            if oid:
                return oid
            notes_page_id = notes_page.get("objectId")
        else:
            notes_page_id = sp.get("notesPageId")
        if not notes_page_id:
            logger.debug("speaker_notes: slide %s has no notesPage/notesPageId", slide_page_id[:12])
            return None
        try:
            notes_page = slides_svc.presentations().pages().get(
                presentationId=pres_id, pageObjectId=notes_page_id
            ).execute()
        except HttpError as e:
            logger.warning("speaker_notes: failed to get notes page for slide %s: %s", slide_page_id[:12], e)
            return None
        oid = (notes_page.get("notesProperties") or {}).get("speakerNotesObjectId")
        if not oid:
            logger.debug("speaker_notes: notes page has no speakerNotesObjectId")
        return oid
    logger.debug("speaker_notes: slide %s not found in presentation", slide_page_id[:12])
    return None


def set_speaker_notes(slides_svc, pres_id: str, slide_page_id: str, notes_text: str) -> bool:
    """Write text to the speaker notes for the given slide. Returns True if successful."""
    oid = get_speaker_notes_object_id(slides_svc, pres_id, slide_page_id)
    if not oid:
        logger.warning("set_speaker_notes: no speaker notes object for slide %s (pres %s)", slide_page_id[:12], pres_id[:12])
        return False
    text = notes_text or ""
    reqs = [
        {"deleteText": {"objectId": oid, "textRange": {"type": "ALL"}}},
        {"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}},
    ]
    try:
        slides_presentations_batch_update(slides_svc, pres_id, reqs)
        return True
    except HttpError as e:
        err_str = str(e)
        if "startIndex 0 must be less than the endIndex 0" in err_str:
            try:
                slides_presentations_batch_update(
                    slides_svc,
                    pres_id,
                    [{"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}}],
                )
                return True
            except HttpError as e2:
                logger.warning("set_speaker_notes: insertText (empty-notes fallback) failed for slide %s: %s", slide_page_id[:12], e2)
                return False
        logger.warning("set_speaker_notes: batchUpdate failed for slide %s: %s", slide_page_id[:12], e)
        return False


def _build_notes_shape_map(slides_svc, pres_id: str) -> dict[str, str]:
    """Single presentations.get -> map of slide_page_id to speakerNotesObjectId."""
    fields = "slides(objectId,slideProperties(notesPage(objectId,notesProperties(speakerNotesObjectId))))"
    pres = slides_svc.presentations().get(
        presentationId=pres_id, fields=fields
    ).execute()
    result: dict[str, str] = {}
    for page in pres.get("slides", []):
        slide_id = page.get("objectId")
        sp = page.get("slideProperties") or {}
        notes_page = sp.get("notesPage")
        if isinstance(notes_page, dict):
            oid = (notes_page.get("notesProperties") or {}).get("speakerNotesObjectId")
            if oid:
                result[slide_id] = oid
                continue
            notes_page_id = notes_page.get("objectId")
        else:
            notes_page_id = sp.get("notesPageId")
        if notes_page_id:
            try:
                np = slides_svc.presentations().pages().get(
                    presentationId=pres_id, pageObjectId=notes_page_id
                ).execute()
                oid = (np.get("notesProperties") or {}).get("speakerNotesObjectId")
                if oid:
                    result[slide_id] = oid
            except HttpError:
                pass
    return result


def set_speaker_notes_batch(
    slides_svc, pres_id: str, items: list[tuple[str, str]]
) -> int:
    """Write speaker notes for many slides in one batchUpdate.

    ``items`` is a list of ``(slide_page_id, notes_text)`` pairs. Returns the
    number of slides successfully mapped into the update request.
    """
    if not items:
        return 0
    notes_map = _build_notes_shape_map(slides_svc, pres_id)
    reqs: list[dict[str, Any]] = []
    mapped = 0
    for slide_id, text in items:
        oid = notes_map.get(slide_id)
        if not oid:
            logger.warning("set_speaker_notes_batch: no notes shape for slide %s", slide_id[:12])
            continue
        reqs.append({"deleteText": {"objectId": oid, "textRange": {"type": "ALL"}}})
        reqs.append({"insertText": {"objectId": oid, "text": text or "", "insertionIndex": 0}})
        mapped += 1
    if not reqs:
        return 0
    try:
        slides_presentations_batch_update(slides_svc, pres_id, reqs)
        return mapped
    except HttpError as e:
        err_str = str(e)
        if "startIndex 0 must be less than the endIndex 0" in err_str:
            insert_only = [r for r in reqs if "insertText" in r]
            try:
                slides_presentations_batch_update(slides_svc, pres_id, insert_only)
                return mapped
            except HttpError as e2:
                logger.warning("set_speaker_notes_batch: insert-only fallback failed: %s", e2)
                return 0
        logger.warning("set_speaker_notes_batch: batchUpdate failed: %s", e)
        return 0
