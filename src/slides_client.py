"""Google Slides client for creating CS-oriented usage report decks.

Auth, batchUpdate throttling, and chunked updates live in ``slides_api``.
Dimensions, brand palette, and shared layout helpers live in ``slides_theme``.
"""

import datetime
import json
import os
import random
import threading
import time
from pathlib import Path
from typing import Any

from googleapiclient.errors import HttpError

from .config import GOOGLE_DRIVE_FOLDER_ID, logger
from .cs_report_client import get_csr_section
from .slide_loader import (
    benchmarks_min_peers_for_cohort_median,
    cohort_findings_min_customers_for_cross_cohort_compare,
    cohort_profiles_max_physical_slides,
)
from .slides_api import (
    GOOGLE_API_TIMEOUT_S,
    SCOPES,
    _build_slides_service_for_thread,
    _get_service,
    _google_api_unreachable_hint,
    presentations_batch_update_chunked,
    slides_presentations_batch_update,
)
from .slides_theme import (
    BLACK,
    BLUE,
    BODY_BOTTOM,
    BODY_Y,
    CONTENT_W,
    DARK,
    FONT,
    FONT_SERIF,
    GRAY,
    LIGHT,
    LTBLUE,
    MARGIN,
    MAX_PAGINATED_SLIDE_PAGES,
    MINT,
    MONO,
    NAVY,
    SLIDE_H,
    SLIDE_PAGINATING_SLIDE_TYPES,
    SLIDE_W,
    TEAL,
    TITLE_Y,
    WHITE,
    _CohortProfileTraceLabels,
    _CohortSummaryLabels,
    _HealthSnapshotLabels,
    _cap_chunk_list,
    _cap_page_count,
    _cohort_summary_metrics,
    _date_range,
    _estimated_body_line_height_pt,
    _fit_kpi_label,
    _list_data_rows_fit_span,
    _single_embedded_chart_layout,
    _table_rows_fit_span,
    _truncate_kpi_card_label,
    KPI_METRIC_LABEL_PT,
    slide_type_may_paginate,
)

# ── Primitives ──

def _sz(w, h):
    return {"width": {"magnitude": w, "unit": "PT"}, "height": {"magnitude": h, "unit": "PT"}}


def _tf(x, y):
    return {"scaleX": 1, "scaleY": 1, "translateX": x, "translateY": y, "unit": "PT"}


def _slide(reqs, sid, idx):
    reqs.append({"createSlide": {"objectId": sid, "insertionIndex": idx}})


def _bg(reqs, sid, color):
    reqs.append({
        "updatePageProperties": {
            "objectId": sid,
            "pageProperties": {"pageBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
            "fields": "pageBackgroundFill",
        }
    })


def _box(reqs, oid, sid, x, y, w, h, text):
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "TEXT_BOX",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    if text:
        reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})


def _wrap_box(reqs, oid, sid, x, y, w, h, text):
    """Text box that clips content to its bounding box (prevents overflow onto neighbours)."""
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "TEXT_BOX",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    # Disable auto-fit so the box stays at the declared height and clips overflow
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {"contentAlignment": "TOP"},
            "fields": "contentAlignment",
        }
    })
    if text:
        reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})


def _rect(reqs, oid, sid, x, y, w, h, fill):
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": fill}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })


def _bar_rect(reqs, oid, sid, x, y, w, h, fill, outline=NAVY):
    """Rectangle for chart bars with a visible outline."""
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": fill}}},
                "outline": {
                    "outlineFill": {"solidFill": {"color": {"rgbColor": outline}}},
                    "weight": {"magnitude": 1, "unit": "PT"},
                },
            },
            "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
        }
    })


def _kpi_metric_card(
    reqs: list,
    oid_base: str,
    sid: str,
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    value: str,
    *,
    accent: dict | None = None,
    label_pt: float = KPI_METRIC_LABEL_PT,
    value_pt: float = 18,
) -> None:
    """Outlined KPI tile for app-built slides; black label, bold accent value. See SLIDE_DESIGN_STANDARDS (app-built scope)."""
    accent = accent or BLUE
    _bar_rect(reqs, oid_base, sid, x, y, w, h, LIGHT, outline=GRAY)
    pad = 10.0
    inner_w = max(40.0, w - 2 * pad)
    label, label_pt = _fit_kpi_label(label, inner_w, label_pt)
    _box(reqs, f"{oid_base}_l", sid, x + pad, y + 8, inner_w, 12, label)
    # Use ALL so styling covers the full text (Slides may reserve index 0 for a paragraph marker;
    # FIXED_RANGE 0..len(label) often leaves the visible run in theme gray).
    if label:
        reqs.append({
            "updateTextStyle": {
                "objectId": f"{oid_base}_l",
                "textRange": {"type": "ALL"},
                "style": {
                    "fontSize": {"magnitude": label_pt, "unit": "PT"},
                    "foregroundColor": {"opaqueColor": {"rgbColor": BLACK}},
                    "fontFamily": FONT,
                },
                "fields": "fontSize,foregroundColor,fontFamily",
            }
        })
    val_h = max(22.0, h - 28.0)
    _box(reqs, f"{oid_base}_v", sid, x + pad, y + 22, inner_w, val_h, value)
    if value:
        reqs.append({
            "updateTextStyle": {
                "objectId": f"{oid_base}_v",
                "textRange": {"type": "ALL"},
                "style": {
                    "bold": True,
                    "fontSize": {"magnitude": value_pt, "unit": "PT"},
                    "foregroundColor": {"opaqueColor": {"rgbColor": accent}},
                    "fontFamily": FONT,
                },
                "fields": "bold,fontSize,foregroundColor,fontFamily",
            }
        })


CHART_LEGEND_PT = 11.0

def _slide_chart_legend(
    reqs: list,
    sid: str,
    oid_prefix: str,
    x: float,
    y: float,
    entries: list[tuple[str, dict]],
    *,
    font_pt: float = CHART_LEGEND_PT,
    swatch_size: float = 10.0,
    gap: float = 6.0,
    entry_gap: float = 18.0,
) -> float:
    """Render a horizontal slide-level chart legend and return y + height consumed.

    *entries* is a list of ``(label, color_dict)`` pairs.  Each entry gets a
    small filled square (swatch) followed by the label text.  This replaces
    Sheets-rendered legends that are too small at presentation scale.
    """
    cursor_x = x
    for i, (label, color) in enumerate(entries):
        sw_oid = f"{oid_prefix}_sw{i}"
        _rect(reqs, sw_oid, sid, cursor_x, y + 2, swatch_size, swatch_size, color)
        cursor_x += swatch_size + gap
        lbl_oid = f"{oid_prefix}_lt{i}"
        _box(reqs, lbl_oid, sid, cursor_x, y, 120, swatch_size + 6, label)
        _style(reqs, lbl_oid, 0, len(label), size=font_pt, color=NAVY, font=FONT)
        cursor_x += len(label) * font_pt * 0.52 + entry_gap
    return y + swatch_size + 8


# ── Speaker notes (notes page per slide) ──────────────────────────────────────


def get_speaker_notes_object_id(slides_svc, pres_id: str, slide_page_id: str) -> str | None:
    """Return the object ID of the speaker-notes shape for the given slide, or None if not found.

    Uses slide's slideProperties.notesPage (embedded) or notesPageId + pages.get for notesProperties.speakerNotesObjectId.
    """
    # Request slides with notes page data so speakerNotesObjectId is included
    _fields = "slides(objectId,slideProperties(notesPage(objectId,notesProperties(speakerNotesObjectId))))"
    pres = slides_svc.presentations().get(
        presentationId=pres_id, fields=_fields
    ).execute()
    for page in pres.get("slides", []):
        if page.get("objectId") != slide_page_id:
            continue
        sp = page.get("slideProperties") or {}
        # API may return notesPage as embedded Page (with notesProperties.speakerNotesObjectId)
        notes_page = sp.get("notesPage")
        if isinstance(notes_page, dict):
            oid = (notes_page.get("notesProperties") or {}).get("speakerNotesObjectId")
            if oid:
                return oid
            # Embedded notes page without speakerNotesObjectId: fetch full page by its objectId
            notes_page_id = notes_page.get("objectId")
        else:
            notes_page_id = sp.get("notesPageId")
        # Fetch notes page by ID when not embedded or when speakerNotesObjectId was missing
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
        # Empty notes shape: deleteText ALL is invalid (startIndex 0 must be < endIndex 0)
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
    """Single ``presentations.get`` → map of ``slide_page_id → speakerNotesObjectId``.

    Falls back to a per-slide ``pages.get`` only when the embedded notesPage
    omits ``speakerNotesObjectId`` (rare).
    """
    _fields = "slides(objectId,slideProperties(notesPage(objectId,notesProperties(speakerNotesObjectId))))"
    pres = slides_svc.presentations().get(
        presentationId=pres_id, fields=_fields
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
    """Write speaker notes for many slides in **one** ``batchUpdate``.

    *items* is a list of ``(slide_page_id, notes_text)`` pairs.
    Returns the number of slides successfully updated.
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


def _collect_jql_soql_trace_entries(obj: Any) -> list[dict[str, str]]:
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
            entries.extend(_collect_jql_soql_trace_entries(val))
        return entries
    if isinstance(obj, list):
        return [e for item in obj for e in _collect_jql_soql_trace_entries(item)]
    return []


def _collect_declared_data_trace_entries(obj: Any) -> list[dict[str, str]]:
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
            entries.extend(_collect_declared_data_trace_entries(val))
        return entries
    if isinstance(obj, list):
        return [e for item in obj for e in _collect_declared_data_trace_entries(item)]
    return []


def _collect_data_trace_entries(obj: Any) -> list[dict[str, str]]:
    """All trace rows: Jira, Salesforce, and declared ``data_traces``."""
    return _collect_jql_soql_trace_entries(obj) + _collect_declared_data_trace_entries(obj)


def _dedupe_data_trace_entries(entries: list[dict[str, str]]) -> list[dict[str, str]]:
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


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_builder_return(ret: Any, default_slide_id: str) -> tuple[int, list[str]]:
    """Slide builders return ``next_idx`` (int) or ``(next_idx, [page_object_id, ...])`` for multi-page slides."""
    if isinstance(ret, tuple) and len(ret) == 2 and isinstance(ret[1], list):
        ids = [str(x) for x in ret[1] if x]
        return int(ret[0]), (ids if ids else [default_slide_id])
    return int(ret), [default_slide_id]


def _health_snapshot_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Account Health Snapshot — description = on-slide label (text before ``:``)."""
    eng = report.get("engagement") or {}
    bench = report.get("benchmarks") or {}
    acct = report.get("account") or {}
    if not eng or not bench or not acct:
        return []
    H = _HealthSnapshotLabels
    rate = eng.get("active_rate_7d")
    cohort_name = (bench.get("cohort_name") or "").strip()
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count") or 0
    min_peers = benchmarks_min_peers_for_cohort_median()
    use_cohort = cohort_med is not None and cohort_n >= min_peers
    if use_cohort:
        vs = rate - cohort_med
        bench_label = f"{cohort_name} median of {cohort_med}%  ({cohort_n} peers)"
    else:
        vs = rate - bench.get("peer_median_rate", 0)
        bench_label = f"all-customer median of {bench.get('peer_median_rate')}%  ({bench.get('peer_count')} peers)"
    direction = "above" if vs > 0 else "below" if vs < 0 else "at"

    rows: list[dict[str, str]] = [
        {
            "description": H.CUSTOMER_USERS,
            "source": "Pendo",
            "query": "account.total_visitors — visitors attributed to this customer (metadata / sitenames rollup)",
        },
        {
            "description": H.ACTIVE_THIS_WEEK,
            "source": "Pendo",
            "query": (
                "engagement.active_7d; on-slide % is active_rate_7d (= active_7d / total_visitors)"
            ),
        },
        {
            "description": H.ACTIVE_THIS_MONTH,
            "source": "Pendo",
            "query": "Sum of active_7d + active_30d engagement buckets (counts on slide)",
        },
        {
            "description": H.DORMANT,
            "source": "Pendo",
            "query": "engagement.dormant — no activity in 30+ days",
        },
        {
            "description": H.WEEKLY_ACTIVE_RATE,
            "source": "Pendo",
            "query": (
                f"Same % as row above; {abs(vs):.0f}pp {direction} {bench_label} "
                f"(cohort from cohorts.yaml when n≥{min_peers}; slides/std-07-benchmarks.yaml rollup_params)"
            ),
        },
        {
            "description": H.SITES,
            "source": "Pendo",
            "query": "account.total_sites from visitor sitenames linked to this customer",
        },
        {
            "description": H.COHORT,
            "source": "Pendo + cohorts.yaml",
            "query": "Label from get_customer_cohort / cohorts.yaml (shows Unclassified when missing)",
        },
    ]
    internal = int(acct.get("internal_visitors") or 0)
    if internal:
        rows.append({
            "description": "Internal staff excluded",
            "source": "Pendo",
            "query": "LeanDNA/internal visitors removed from customer engagement totals",
        })
    return rows


def _peer_benchmarks_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Peer Benchmarks — KPI card labels and body lines match ``_benchmarks_slide``."""
    bench = report.get("benchmarks") or {}
    if not bench:
        return []
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    cohort_name = bench.get("cohort_name", "")
    min_peers = benchmarks_min_peers_for_cohort_median()
    use_cohort = cohort_med is not None and cohort_n >= min_peers

    q_rate = (
        "active_7d / total_visitors over the report window; "
        "7-day activity from visitor time-bucket aggregation"
    )
    q_all_median = (
        "Median of weekly active rate across accounts with Pendo data in the same period "
        "(peer_count on payload)"
    )
    q_cohort_median = (
        "Median among accounts in the same manufacturing cohort "
        f"(get_customer_cohort / cohorts.yaml); shown when cohort n≥{min_peers} "
        "(rollup_params on slides/std-07-benchmarks.yaml)"
    )
    q_delta = (
        "Customer weekly active rate minus comparison median (percentage points vs peer/cohort on slide)"
    )
    q_acct = (
        "account.total_visitors and account.total_sites for the account size line under KPI row"
    )

    out: list[dict[str, str]] = [
        {"description": "Weekly active rate (this account)", "source": "Pendo", "query": q_rate},
    ]
    if use_cohort:
        med_lbl = _truncate_kpi_card_label(f"{cohort_name} median ({cohort_n} accounts)")
        out.append({"description": med_lbl, "source": "Pendo", "query": q_cohort_median})
        all_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
        out.append({"description": all_lbl, "source": "Pendo", "query": q_all_median})
    else:
        med_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
        out.append({"description": med_lbl, "source": "Pendo", "query": q_all_median})

    out.append({"description": "Delta", "source": "Pendo", "query": q_delta})
    out.append({"description": "Account size", "source": "Pendo", "query": q_acct})
    return out


def _fmt_platform_value_dollar(v: float) -> str:
    av = abs(float(v))
    if av >= 1_000_000_000:
        return f"${v / 1_000_000_000:,.2f}B"
    if av >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if av >= 1_000:
        return f"${v / 1_000:,.0f}K"
    return f"${v:,.0f}"


def _fmt_platform_value_count(v: int | float) -> str:
    n = int(v)
    an = abs(n)
    if an >= 1_000_000:
        return f"{n / 1_000_000:,.1f}M"
    if an >= 100_000:
        return f"{n / 1_000:,.0f}K"
    return f"{n:,}"


def _platform_value_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Platform Value & ROI — KPI labels match ``_platform_value_slide``; values echoed in query."""
    cs = get_csr_section(report).get("platform_value")
    if not isinstance(cs, dict) or cs.get("error"):
        return []
    ts = float(cs.get("total_savings") or 0)
    to = float(cs.get("total_open_ia_value") or 0)
    tr = int(cs.get("total_recs_created_30d") or 0)
    tp = int(cs.get("total_pos_placed_30d") or 0)
    td = int(cs.get("total_overdue_tasks") or 0)
    fc = int(cs.get("factory_count") or 0)
    site_list = cs.get("sites") or []
    factory_rows = [s for s in site_list if s.get("savings_current_period") or s.get("recs_created_30d")]
    n_tab = len(factory_rows)

    rows: list[dict[str, str]] = [
        {
            "description": "Savings achieved",
            "source": "CS Report",
            "query": (
                f"On-slide value {_fmt_platform_value_dollar(ts)} — sum of "
                "inventoryActionCurrentReportingPeriodSavings endValue across customer week rows "
                f"({fc} factories)"
            ),
        },
        {
            "description": "Open IA pipeline",
            "source": "CS Report",
            "query": (
                f"On-slide value {_fmt_platform_value_dollar(to)} — sum of "
                "inventoryActionOpenValue endValue across customer week rows"
            ),
        },
        {
            "description": "Recs created (30d)",
            "source": "CS Report",
            "query": (
                f"On-slide value {_fmt_platform_value_count(tr)} — sum of "
                "recsCreatedLast30DaysCt endValue across customer week rows"
            ),
        },
        {
            "description": "POs placed (30d)",
            "source": "CS Report",
            "query": (
                f"Shown in gray subline as {tp:,} POs placed — sum of "
                "posPlacedInLast30DaysCt endValue across customer week rows"
            ),
        },
        {
            "description": "Overdue tasks",
            "source": "CS Report",
            "query": (
                f"Shown in gray subline as {td:,} overdue tasks — sum of "
                "workbenchOverdueTasksCt endValue across customer week rows"
            ),
        },
        {
            "description": "Factory",
            "source": "CS Report",
            "query": (
                f"Table column; {n_tab} site row(s) with savings or recs; values from factoryName per week row"
            ),
        },
        {
            "description": "Savings",
            "source": "CS Report",
            "query": (
                "Table column — per-site savings_current_period (same KPI field as headline Savings achieved)"
            ),
        },
        {
            "description": "Recs (30d)",
            "source": "CS Report",
            "query": (
                "Table column — per-site recs_created_30d (same KPI field as headline Recs created (30d))"
            ),
        },
    ]
    return rows


def _support_health_exec_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Support Health Summary (exec)."""
    jira = report.get("jira")
    if not isinstance(jira, dict) or jira.get("error") or jira.get("total_issues", 0) == 0:
        return []
    total = jira["total_issues"]
    open_n = jira.get("open_issues", 0)
    esc = jira.get("escalated", 0)
    ttfr = jira.get("ttfr", {})
    ttr = jira.get("ttr", {})
    rows: list[dict[str, str]] = [
        {"description": "Open tickets", "source": "Jira (HELP)",
         "query": f"On-slide value {open_n} — open issues in HELP project (of {total} total in period)"},
        {"description": "Escalated", "source": "Jira (HELP)",
         "query": f"On-slide value {esc} — issues with escalation flag"},
        {"description": "TTFR (median)", "source": "Jira (HELP)",
         "query": f"On-slide value {ttfr.get('median', '—')} — median time to first response across {ttfr.get('measured', 0)} measured tickets"},
        {"description": "TTR (median)", "source": "Jira (HELP)",
         "query": f"On-slide value {ttr.get('median', '—')} — median time to resolution across {ttr.get('measured', 0)} measured tickets"},
    ]
    sentiment = jira.get("by_sentiment", {})
    if sentiment:
        parts = [f"{k}: {v}" for k, v in sentiment.items() if k != "Unknown"]
        rows.append({"description": "Sentiment", "source": "Jira (HELP)",
                     "query": f"Ticket sentiment breakdown — {', '.join(parts)}"})
    return rows


def _salesforce_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Salesforce Pipeline (exec)."""
    sf = report.get("salesforce")
    if not isinstance(sf, dict) or not sf.get("matched"):
        return []
    opp = sf.get("opportunity_count_this_year", 0)
    arr = sf.get("pipeline_arr", 0)
    n_accts = len(sf.get("accounts", []))
    return [
        {"description": "Pipeline ARR", "source": "Salesforce",
         "query": f"On-slide value ${arr:,.0f} — sum of Amount on open Opportunity records for matched accounts"},
        {"description": "Opportunities (this year)", "source": "Salesforce",
         "query": f"On-slide value {opp} — count of Opportunity records with CloseDate in current fiscal year"},
        {"description": "SF accounts matched", "source": "Salesforce",
         "query": f"On-slide value {n_accts} — Customer Entity accounts matched by name search"},
    ]


def _platform_risk_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Platform Risk slide (health distribution + supply chain)."""
    rows: list[dict[str, str]] = []
    csr = get_csr_section(report)
    cs_ph = csr.get("platform_health")
    if isinstance(cs_ph, dict) and not cs_ph.get("error"):
        dist = cs_ph.get("health_distribution", {})
        fc = cs_ph.get("factory_count", 0)
        rows.extend([
            {"description": "Factory count", "source": "CS Report",
             "query": f"On-slide value {fc} — number of factory rows in CS Report platform health"},
            {"description": "Health distribution", "source": "CS Report",
             "query": f"GREEN={dist.get('GREEN', 0)} YELLOW={dist.get('YELLOW', 0)} RED={dist.get('RED', 0)} — health_score band per factory"},
            {"description": "Critical shortages", "source": "CS Report",
             "query": f"On-slide value {cs_ph.get('total_critical_shortages', 0)} — sum of criticalShortagesCt across factories"},
        ])
    cs_sc = csr.get("supply_chain")
    if isinstance(cs_sc, dict) and not cs_sc.get("error"):
        t = cs_sc.get("totals", {})
        rows.extend([
            {"description": "Total on-hand", "source": "CS Report",
             "query": f"On-slide value ${t.get('total_on_hand', 0):,.0f} — sum of onHandValue across factories"},
            {"description": "Excess inventory", "source": "CS Report",
             "query": f"On-slide value ${t.get('total_excess', 0):,.0f} — sum of excessValue across factories"},
            {"description": "Late POs", "source": "CS Report",
             "query": f"On-slide value {t.get('total_late_pos', 0):,} — sum of latePosCt across factories"},
        ])
    return rows


def _cohort_summary_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Cohort Summary — one ``description: source - query`` line per KPI card."""
    m = _cohort_summary_metrics(report)
    if not m:
        return []
    L = _CohortSummaryLabels
    total_arr = m["total_arr"]
    arr_echo = f"${total_arr:,.0f}" if total_arr > 0 else "—"
    med_login = m["med_login"]
    med_write = m["med_write"]
    med_exports = m["med_exports"]
    med_kei = m["med_kei"]
    return [
        {
            "description": L.TOTAL_CUSTOMERS,
            "source": "Pendo",
            "query": f"On-slide value {m['total_customers']} — customer_count in portfolio report (cohort_digest scope)",
        },
        {
            "description": L.COHORTS,
            "source": "Pendo",
            "query": f"On-slide value {m['num_cohorts']} — cohort buckets with ≥1 customer (get_customer_cohort / cohorts.yaml)",
        },
        {
            "description": L.TOTAL_ARR,
            "source": "Salesforce",
            "query": (
                f"On-slide value {arr_echo} — sum of Account ARR__c for matched customers "
                f"(Name / LeanDNA_Entity_Name__c / Parent / Ultimate Parent); "
                f"{len(report.get('_arr_by_customer') or {})} accounts matched"
            ),
        },
        {
            "description": L.TOTAL_USERS,
            "source": "Pendo",
            "query": f"On-slide value {m['total_users']:,} — sum of total_users across cohort_digest buckets",
        },
        {
            "description": L.ACTIVE_USERS_7D,
            "source": "Pendo",
            "query": f"On-slide value {m['total_active']:,} — sum of total_active_users (7d) across cohort_digest buckets",
        },
        {
            "description": L.ACTIVE_RATE,
            "source": "Pendo",
            "query": (
                f"On-slide value {m['overall_active_pct']}% — 100 × active_users / total_users "
                "(portfolio-wide across cohorts)"
            ),
        },
        {
            "description": L.WEEKLY_ACTIVE_MEDIAN,
            "source": "Pendo",
            "query": (
                f"On-slide value {med_login}% — median of per-cohort median_login_pct "
                "(each cohort median is across its customers’ engagement.active_rate_7d)"
                if med_login is not None
                else "On-slide — — median of per-cohort median_login_pct (insufficient data)"
            ),
        },
        {
            "description": L.WRITE_RATIO_MEDIAN,
            "source": "Pendo",
            "query": (
                f"On-slide value {med_write}% — median of per-cohort median_write_ratio "
                "(depth.write_ratio per customer, median within cohort, then median across cohorts)"
                if med_write is not None
                else "On-slide — — median of per-cohort write ratios (insufficient data)"
            ),
        },
        {
            "description": L.KEI_ADOPTION_MEDIAN,
            "source": "Pendo",
            "query": (
                f"On-slide value {med_kei}% — median of per-cohort kei_adoption_pct "
                "(% of customers in bucket with ≥1 Kei query)"
                if med_kei is not None
                else "On-slide — — median of per-cohort Kei adoption (insufficient data)"
            ),
        },
        {
            "description": L.EXPORTS_MEDIAN,
            "source": "Pendo",
            "query": (
                f"On-slide value {med_exports:.0f} — median of per-cohort median_exports "
                "(exports.total_exports per customer, 30d window, median within cohort then across cohorts)"
                if med_exports is not None
                else "On-slide — — median of per-cohort export medians (insufficient data)"
            ),
        },
        {
            "description": L.LARGEST_COHORT,
            "source": "Pendo",
            "query": f"On-slide value {m['biggest_lbl']} — cohort_digest bucket with max customer count",
        },
    ]


def _cohort_profile_pipeline_rows_for_block(
    report: dict[str, Any],
    block: dict[str, Any],
    *,
    cohort_label: str,
) -> list[dict[str, str]]:
    """One ``description: source - query`` line per on-slide metric for a single cohort bucket."""
    L = _CohortProfileTraceLabels
    name = block.get("display_name", cohort_label)
    n = int(block.get("n") or 0)
    ta = int(block.get("total_active_users") or 0)
    tu = int(block.get("total_users") or 0)
    mlogin = block.get("median_login_pct")
    mw = block.get("median_write_ratio")
    kei_pct = block.get("kei_adoption_pct", 0)
    mex = block.get("median_exports")

    mlogin_os = "On-slide —" if mlogin is None else f"On-slide {mlogin}%"
    mw_os = "On-slide —" if mw is None else f"On-slide {mw}%"
    mex_os = "On-slide —" if mex is None else f"On-slide {mex:.0f}"

    rows: list[dict[str, str]] = [
        {
            "description": f"Cohort profile: {name} ({n} customers)",
            "source": "Pendo",
            "query": (
                f"Bucket {cohort_label!r} in cohort_digest — "
                "get_customer_cohort / cohorts.yaml; portfolio rollup customer summaries"
            ),
        },
        {
            "description": L.ACTIVE_USERS_7D,
            "source": "Pendo",
            "query": (
                f"On-slide cohort total {ta:,} — cohort_digest.total_active_users "
                f"({name})"
            ),
        },
        {
            "description": L.TOTAL_USERS,
            "source": "Pendo",
            "query": (
                f"On-slide cohort total {tu:,} — cohort_digest.total_users ({name})"
            ),
        },
        {
            "description": L.WEEKLY_ACTIVE_MEDIAN,
            "source": "Pendo",
            "query": (
                f"{mlogin_os} — median of engagement.active_rate_7d across customers "
                f"in this cohort ({name})"
            ),
        },
        {
            "description": L.WRITE_RATIO_MEDIAN,
            "source": "Pendo",
            "query": (
                f"{mw_os} — median of depth.write_ratio per customer in cohort ({name})"
            ),
        },
        {
            "description": L.KEI_ADOPTERS_PCT,
            "source": "Pendo",
            "query": (
                f"On-slide {kei_pct}% — share of customers in cohort with ≥1 Kei query ({name})"
            ),
        },
        {
            "description": L.EXPORTS_MEDIAN,
            "source": "Pendo",
            "query": (
                f"{mex_os} — median exports.total_exports (30d) per customer in cohort ({name})"
            ),
        },
    ]

    arr_map = report.get("_arr_by_customer") or {}
    customers = block.get("customers") or []
    cohort_arr = sum(float(arr_map.get(c, 0) or 0) for c in customers)
    n_matched = sum(1 for c in customers if float(arr_map.get(c, 0) or 0) > 0)
    if cohort_arr > 0:
        rows.append({
            "description": L.TOTAL_ARR,
            "source": "Salesforce",
            "query": (
                f"On-slide {_fmt_platform_value_dollar(cohort_arr)} — sum Account.ARR__c for "
                f"{n_matched}/{len(customers)} cohort customers with matches ({name}); "
                "Name / LeanDNA_Entity_Name__c / Parent / Ultimate Parent match"
            ),
        })
    return rows


def _cohort_profiles_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Cohort Profile slide(s) — one trace line per metric; scoped per page when set."""
    entry = report.get("_speaker_note_slide_entry")
    entry = entry if isinstance(entry, dict) else {}
    scoped = entry.get("_cohort_profile_block")
    if isinstance(scoped, dict) and int(scoped.get("n") or 0) > 0:
        cid = str(scoped.get("cohort_id") or scoped.get("display_name") or "bucket")
        return _cohort_profile_pipeline_rows_for_block(report, scoped, cohort_label=cid)

    digest = report.get("cohort_digest") or {}
    if not digest:
        return []
    rows: list[dict[str, str]] = []
    ordered = sorted(
        digest.items(),
        key=lambda kv: (kv[0] == "unclassified", -int((kv[1] or {}).get("n") or 0) if isinstance(kv[1], dict) else 0),
    )
    for cid, block in ordered:
        if not isinstance(block, dict) or not int(block.get("n") or 0):
            continue
        rows.extend(_cohort_profile_pipeline_rows_for_block(report, block, cohort_label=str(cid)))

    arr_map = report.get("_arr_by_customer") or {}
    if arr_map:
        n_with = len(arr_map)
        total_arr = sum(arr_map.values())
        rows.append({
            "description": "ARR by customer (portfolio)",
            "source": "Salesforce (Account.ARR__c)",
            "query": (
                f"Matched {n_with} customers with ARR totalling ${total_arr:,.0f} — "
                "single batch query on Entity accounts, matched by Name / LeanDNA_Entity_Name__c / Parent / Ultimate Parent"
            ),
        })
    return rows


def _cohort_findings_pipeline_traces(report: dict[str, Any]) -> list[dict[str, str]]:
    """Speaker-note rows for Cohort Findings slide."""
    bullets = report.get("cohort_findings_bullets") or []
    if not bullets:
        return []
    _n = cohort_findings_min_customers_for_cross_cohort_compare()
    return [{
        "description": "Cohort findings",
        "source": "Pendo (compute_cohort_portfolio_rollup)",
        "query": (
            f"{len(bullets)} bullet(s): portfolio totals, per-cohort medians (login, write, exports, Kei), "
            f"cross-cohort spreads (cohorts with n≥{_n} only; slides/cohort-02-findings.yaml rollup_params) — "
            "from full portfolio customer summaries in this report"
        ),
    }]


_SLIDE_CANONICAL_PIPELINE_TRACES: dict[str, Any] = {
    "health": _health_snapshot_pipeline_traces,
    "benchmarks": _peer_benchmarks_pipeline_traces,
    "platform_value": _platform_value_pipeline_traces,
    "support_health_exec": _support_health_exec_pipeline_traces,
    "salesforce_pipeline": _salesforce_pipeline_traces,
    "platform_risk": _platform_risk_pipeline_traces,
    "cohort_summary": _cohort_summary_pipeline_traces,
    "cohort_profiles": _cohort_profiles_pipeline_traces,
    "cohort_findings": _cohort_findings_pipeline_traces,
}


def _build_slide_jql_speaker_notes(report: dict[str, Any], entry: dict[str, Any]) -> str:
    """Speaker notes: timestamp; slide id/type; blank line; trace lines only (description: source - query)—no section heading."""
    from datetime import datetime

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

        required_keys = SLIDE_DATA_REQUIREMENTS.get(slide_type, [])
        canon_fn = _SLIDE_CANONICAL_PIPELINE_TRACES.get(slide_type)

        pipeline: list[dict[str, str]] = []
        if canon_fn is not None:
            pipeline = canon_fn(report)
        elif required_keys:
            for key in required_keys:
                pipeline.extend(_collect_declared_data_trace_entries(report.get(key)))
            pipeline = _dedupe_data_trace_entries(pipeline)
        else:
            pipeline = _dedupe_data_trace_entries(_collect_declared_data_trace_entries(report))

        executable: list[dict[str, str]] = []
        if required_keys:
            for key in required_keys:
                executable.extend(_collect_jql_soql_trace_entries(report.get(key)))
        else:
            executable = _collect_jql_soql_trace_entries(report)
        executable = _dedupe_data_trace_entries(executable)

        entries = _dedupe_data_trace_entries(pipeline + executable)

        if not entries:
            if slide_type in ("salesforce_comprehensive_cover", "salesforce_category"):
                header.append("")
                header.append(
                    "Live Salesforce metrics: Salesforce - SOQL via REST API (per-object queries not recorded in this payload)"
                )
            return "\n".join(header)

        header.append("")
        for e in entries:
            desc = e.get("description") or "Data"
            src = e.get("source") or "Unknown"
            q = e.get("query") or ""
            header.append(f"{desc}: {src} - {q}")
        return "\n".join(header)
    finally:
        if prev_sn_entry is not None:
            report["_speaker_note_slide_entry"] = prev_sn_entry
        else:
            report.pop("_speaker_note_slide_entry", None)


def _pill(reqs, oid, sid, x, y, w, h, text, bg, fg):
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "ROUND_RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": bg}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })
    reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})
    _style(reqs, oid, 0, len(text), bold=True, size=11, color=fg)
    _align(reqs, oid, "CENTER")


def _style(reqs, oid, start, end, bold=False, size=None, color=None, font=None, italic=False,
           link=None):
    if start >= end:
        return
    s: dict[str, Any] = {}
    f = []
    if bold:
        s["bold"] = True; f.append("bold")
    if italic:
        s["italic"] = True; f.append("italic")
    if size:
        s["fontSize"] = {"magnitude": size, "unit": "PT"}; f.append("fontSize")
    if color:
        s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
    if font:
        s["fontFamily"] = font; f.append("fontFamily")
    if link:
        s["link"] = {"url": link}; f.append("link")
    if f:
        reqs.append({
            "updateTextStyle": {
                "objectId": oid,
                "textRange": {"type": "FIXED_RANGE", "startIndex": start, "endIndex": end},
                "style": s, "fields": ",".join(f),
            }
        })


def _align(reqs, oid, alignment):
    reqs.append({
        "updateParagraphStyle": {
            "objectId": oid,
            "textRange": {"type": "ALL"},
            "style": {"alignment": alignment},
            "fields": "alignment",
        }
    })


# Red banner for "data not available" (also recorded in QA for Data Quality slide)
_BANNER_RED = {"red": 0.9, "green": 0.2, "blue": 0.2}


def _red_banner(reqs, oid, sid, x, y, w, h, text):
    """Create a red rectangle with white bold centered text (data-missing banner)."""
    reqs.append({
        "createShape": {
            "objectId": oid,
            "shapeType": "ROUND_RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": _BANNER_RED}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })
    reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})
    _style(reqs, oid, 0, len(text), bold=True, size=12, color=WHITE, font=FONT)
    _align(reqs, oid, "CENTER")


def _missing_data_slide(reqs, sid, report, idx, missing_description):
    """Render a slide with title + red banner when required data is unavailable; flag for Data Quality."""
    from .qa import qa
    entry = report.get("_current_slide") or {}
    slide_type = entry.get("slide_type", entry.get("id", "slide"))
    slide_title = entry.get("title", slide_type.replace("_", " ").title())

    report.setdefault("_missing_slide_data", []).append({
        "slide_type": slide_type,
        "slide_title": slide_title,
        "missing": missing_description,
    })
    qa.flag(
        f"Slide \"{slide_title}\": {missing_description} not available",
        severity="warning",
        internal=False,
    )

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, slide_title)
    banner_text = f"Data not available: {missing_description}"
    if len(banner_text) > 90:
        banner_text = banner_text[:87] + "..."
    _red_banner(reqs, f"{sid}_banner", sid, MARGIN, BODY_Y - 8, CONTENT_W, 28, banner_text)
    return idx + 1


def _internal_footer(reqs, sid):
    label = "INTERNAL ONLY"
    fid = f"{sid}_iof"
    _box(reqs, fid, sid, SLIDE_W - MARGIN - 80, SLIDE_H - 16, 80, 12, label)
    _style(reqs, fid, 0, len(label), size=6, color=GRAY, font=FONT)
    reqs.append({
        "updateParagraphStyle": {
            "objectId": fid,
            "textRange": {"type": "ALL"},
            "style": {"alignment": "END"},
            "fields": "alignment",
        }
    })


def _clean_table(reqs, table_id, num_rows, num_cols):
    """Strip all borders from a table, then add a thin blue header separator."""
    reqs.append({
        "updateTableBorderProperties": {
            "objectId": table_id,
            "tableRange": {
                "location": {"rowIndex": 0, "columnIndex": 0},
                "rowSpan": num_rows, "columnSpan": num_cols,
            },
            "borderPosition": "ALL",
            "tableBorderProperties": {
                "tableBorderFill": {"solidFill": {"color": {"rgbColor": WHITE}}},
                "weight": {"magnitude": 0.01, "unit": "PT"},
                "dashStyle": "SOLID",
            },
            "fields": "tableBorderFill,weight,dashStyle",
        }
    })
    reqs.append({
        "updateTableBorderProperties": {
            "objectId": table_id,
            "tableRange": {
                "location": {"rowIndex": 0, "columnIndex": 0},
                "rowSpan": 1, "columnSpan": num_cols,
            },
            "borderPosition": "BOTTOM",
            "tableBorderProperties": {
                "tableBorderFill": {"solidFill": {"color": {"rgbColor": BLUE}}},
                "weight": {"magnitude": 1, "unit": "PT"},
                "dashStyle": "SOLID",
            },
            "fields": "tableBorderFill,weight,dashStyle",
        }
    })


def _simple_table(reqs, table_id, sid, x, y, col_widths, row_h, headers, rows):
    """Create a styled table with headers and data rows.

    Returns the total height consumed so callers can position elements below.
    """
    num_rows = 1 + len(rows)
    num_cols = len(headers)
    tbl_w = sum(col_widths)
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(tbl_w, num_rows * row_h),
                "transform": _tf(x, y),
            },
            "rows": num_rows, "columns": num_cols,
        }
    })

    def _ct(row, col, text):
        if text:
            reqs.append({"insertText": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "text": str(text), "insertionIndex": 0,
            }})

    def _cs(row, col, length, **kwargs):
        if length > 0:
            reqs.append({"updateTextStyle": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": length},
                "style": {k: v for k, v in {
                    "bold": kwargs.get("bold"), "fontSize": {"magnitude": kwargs.get("size", 9), "unit": "PT"},
                    "foregroundColor": {"opaqueColor": {"rgbColor": kwargs.get("color", NAVY)}} if kwargs.get("color") else None,
                    "fontFamily": kwargs.get("font", FONT),
                }.items() if v is not None},
                "fields": ",".join(f for f in ["bold", "fontSize", "foregroundColor", "fontFamily"] if kwargs.get(f.replace("fontSize", "size").replace("foregroundColor", "color").replace("fontFamily", "font"), None) is not None or f in ("fontSize", "fontFamily")),
            }})

    for ci, h in enumerate(headers):
        _ct(0, ci, h)
        _cs(0, ci, len(str(h)), bold=True, size=9, color=WHITE, font=FONT)

    for ri, row in enumerate(rows):
        for ci, val in enumerate(row):
            _ct(ri + 1, ci, str(val))
            _cs(ri + 1, ci, len(str(val)), size=9, color=NAVY, font=FONT)

    for ci, w in enumerate(col_widths):
        reqs.append({"updateTableColumnProperties": {
            "objectId": table_id, "columnIndices": [ci],
            "tableColumnProperties": {"columnWidth": {"magnitude": w, "unit": "PT"}},
            "fields": "columnWidth",
        }})

    _clean_table(reqs, table_id, num_rows, num_cols)

    for ci in range(num_cols):
        reqs.append({"updateTableCellProperties": {
            "objectId": table_id,
            "tableRange": {"location": {"rowIndex": 0, "columnIndex": ci}, "rowSpan": 1, "columnSpan": 1},
            "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": NAVY}}}},
            "fields": "tableCellBackgroundFill",
        }})

    return num_rows * row_h


def _table_cell_bg(reqs, table_id, row, col, color):
    """Set background color on a single table cell."""
    reqs.append({"updateTableCellProperties": {
        "objectId": table_id,
        "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
        "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
        "fields": "tableCellBackgroundFill",
    }})


def _omission_note(reqs, sid, omitted_names: list[str], label: str = "Not shown"):
    """Add a small italic note near the bottom listing items omitted for space."""
    if not omitted_names:
        return
    names = ", ".join(omitted_names[:8])
    if len(omitted_names) > 8:
        names += f", +{len(omitted_names) - 8} more"
    note = f"{label}: {names}"
    oid = f"{sid}_omit"
    _box(reqs, oid, sid, MARGIN, BODY_BOTTOM - 2, CONTENT_W, 14, note)
    _style(reqs, oid, 0, len(note), size=7, color=GRAY, font=FONT, italic=True)


def _slide_title(reqs, sid, text):
    """Standard content-slide title: navy text + teal underline + internal footer."""
    title_len = len(text or "")
    if title_len > 100:
        title_size = 12
    elif title_len > 85:
        title_size = 13
    elif title_len > 72:
        title_size = 14
    elif title_len > 60:
        title_size = 16
    else:
        title_size = 20
    oid = f"{sid}_ttl"
    _box(reqs, oid, sid, MARGIN, TITLE_Y, CONTENT_W, 36, text)
    _style(reqs, oid, 0, len(text), bold=True, size=title_size, color=NAVY, font=FONT_SERIF)
    _rect(reqs, f"{sid}_ul", sid, MARGIN, TITLE_Y + 38, 56, 2.5, BLUE)
    _internal_footer(reqs, sid)


# ── Slide builders ──

def _title_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    acct = report["account"]
    name = report["customer"]
    sub = f"Product Usage Review  ·  {_date_range(report['days'], report.get('quarter'), report.get('quarter_start'), report.get('quarter_end'))}"
    meta = f"CSM: {acct['csm']}  |  {acct['total_sites']} sites · {acct['total_visitors']} users  |  {report['generated']}"

    _rect(reqs, f"{sid}_bar", sid, 0, 190, SLIDE_W, 3, BLUE)

    _box(reqs, f"{sid}_n", sid, MARGIN, 100, CONTENT_W, 60, name)
    _style(reqs, f"{sid}_n", 0, len(name), bold=True, size=40, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 200, CONTENT_W, 30, sub)
    _style(reqs, f"{sid}_s", 0, len(sub), size=15, color=BLUE, font=FONT)

    _box(reqs, f"{sid}_m", sid, MARGIN, 350, CONTENT_W, 24, meta)
    _style(reqs, f"{sid}_m", 0, len(meta), size=9, color=GRAY, font=FONT)

    label = "INTERNAL ONLY"
    _box(reqs, f"{sid}_int", sid, MARGIN, 160, CONTENT_W, 22, label)
    _style(reqs, f"{sid}_int", 0, len(label), bold=True, size=10, color=BLUE, font=FONT)

    return idx + 1


# ── Composite health scoring ──

_HEALTH_GOOD = {"red": 0.10, "green": 0.55, "blue": 0.28}   # green
_HEALTH_MOD  = BLUE                                            # blue
_HEALTH_BAD  = {"red": 0.78, "green": 0.18, "blue": 0.18}    # red
_HEALTH_NA   = GRAY                                            # no data

_SCORE_MAP = {"HEALTHY": 3, "MODERATE": 2, "AT RISK": 1}
_LABEL_FROM_SCORE = {3: "HEALTHY", 2: "MODERATE", 1: "AT RISK"}
_COLOR_FROM_LABEL = {"HEALTHY": _HEALTH_GOOD, "MODERATE": _HEALTH_MOD, "AT RISK": _HEALTH_BAD}


def _score_engagement(report: dict) -> tuple[str, str]:
    """Score user-engagement health from Pendo active rate. Returns (label, rationale)."""
    rate = report.get("engagement", {}).get("active_rate_7d", 0)
    if rate >= 40:
        return "HEALTHY", f"{rate}% weekly active"
    elif rate >= 20:
        return "MODERATE", f"{rate}% weekly active"
    else:
        return "AT RISK", f"{rate}% weekly active"


def _score_platform(report: dict) -> tuple[str, str] | None:
    """Score platform health from CS Report factory health scores. Returns None if no data."""
    cs = get_csr_section(report).get("platform_health") or {}
    sites = cs.get("sites", [])
    if not sites:
        return None
    dist = cs.get("health_distribution", {})
    reds = dist.get("RED", 0)
    greens = dist.get("GREEN", 0)
    total = len(sites)
    pct_green = greens / max(total, 1) * 100
    if reds > 0:
        return "AT RISK", f"{reds} RED factory{'s' if reds != 1 else ''}"
    elif pct_green >= 50:
        return "HEALTHY", f"{greens}/{total} factories GREEN"
    else:
        return "MODERATE", f"{greens}/{total} factories GREEN"


def _score_support(report: dict) -> tuple[str, str] | None:
    """Score support health from Jira ticket data. Returns None if no data."""
    jira = report.get("jira", {})
    if not jira or jira.get("error") or jira.get("total_issues", 0) == 0:
        return None
    total = jira["total_issues"]
    escalated = jira.get("escalated", 0)
    open_n = jira.get("open_issues", 0)
    ttr = jira.get("ttr", {})
    breached = ttr.get("breached", 0)

    esc_pct = escalated / max(total, 1) * 100
    open_pct = open_n / max(total, 1) * 100

    if breached > 0 or esc_pct > 40:
        return "AT RISK", f"{escalated} escalated, {breached} SLA breach{'es' if breached != 1 else ''}"
    elif esc_pct > 20 or open_pct > 50:
        return "MODERATE", f"{open_n} open, {escalated} escalated"
    else:
        return "HEALTHY", f"{open_n} open, {escalated} escalated"


def _composite_health(report: dict) -> dict[str, Any]:
    """Compute composite health from all available dimensions."""
    dims: list[dict[str, Any]] = []

    eng_label, eng_why = _score_engagement(report)
    dims.append({"name": "Engagement", "label": eng_label, "detail": eng_why,
                 "source": "Pendo", "color": _COLOR_FROM_LABEL[eng_label]})

    plat = _score_platform(report)
    if plat:
        dims.append({"name": "Platform", "label": plat[0], "detail": plat[1],
                      "source": "CS Report", "color": _COLOR_FROM_LABEL[plat[0]]})

    supp = _score_support(report)
    if supp:
        dims.append({"name": "Support", "label": supp[0], "detail": supp[1],
                      "source": "Jira", "color": _COLOR_FROM_LABEL[supp[0]]})

    scores = [_SCORE_MAP[d["label"]] for d in dims]
    avg = sum(scores) / len(scores) if scores else 2
    if avg >= 2.5:
        overall = "HEALTHY"
    elif avg >= 1.5:
        overall = "MODERATE"
    else:
        overall = "AT RISK"

    return {
        "overall": overall,
        "overall_color": _COLOR_FROM_LABEL[overall],
        "dimensions": dims,
    }


def _health_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Account Health Snapshot")

    eng = report["engagement"]
    bench = report["benchmarks"]
    acct = report["account"]
    rate = eng["active_rate_7d"]
    active = eng["active_7d"] + eng["active_30d"]
    internal = acct.get("internal_visitors", 0)

    # Composite health badge
    health = _composite_health(report)
    label = health["overall"]
    badge_bg = health["overall_color"]
    _pill(reqs, f"{sid}_badge", sid, SLIDE_W - MARGIN - 110, TITLE_Y + 2, 110, 28, label, badge_bg, WHITE)

    # KPIs — use cohort benchmark when available
    cohort_name = bench.get("cohort_name", "")
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    if cohort_med is not None and cohort_n >= benchmarks_min_peers_for_cohort_median():
        vs = rate - cohort_med
        direction = "above" if vs > 0 else "below" if vs < 0 else "at"
        bench_label = f"{cohort_name} median of {cohort_med}%  ({cohort_n} peers)"
    else:
        vs = rate - bench["peer_median_rate"]
        direction = "above" if vs > 0 else "below" if vs < 0 else "at"
        bench_label = f"all-customer median of {bench['peer_median_rate']}%  ({bench['peer_count']} peers)"
    H = _HealthSnapshotLabels
    lines = [
        f"{H.CUSTOMER_USERS}: {acct['total_visitors']}",
        f"{H.ACTIVE_THIS_WEEK}: {eng['active_7d']}  ({rate}%)",
        f"{H.ACTIVE_THIS_MONTH}: {active}",
        f"{H.DORMANT}: {eng['dormant']}",
        "",
        f"{H.WEEKLY_ACTIVE_RATE}: {rate}%  ({abs(vs):.0f}pp {direction} {bench_label})",
        f"{H.SITES}: {acct['total_sites']}  |  {H.COHORT}: {cohort_name or 'Unclassified'}",
    ]
    if internal:
        lines.append(f"({internal} internal staff excluded)")
    kpi = "\n".join(lines)

    _box(reqs, f"{sid}_kpi", sid, MARGIN, BODY_Y, CONTENT_W // 2 + 20, 200, kpi)
    _style(reqs, f"{sid}_kpi", 0, len(kpi), size=12, color=NAVY, font=FONT)

    off = 0
    for line in lines:
        if ":" in line and line.strip() and not line.startswith("("):
            c = line.index(":")
            _style(reqs, f"{sid}_kpi", off, off + c + 1, bold=True)
        off += len(line) + 1

    # Dimension breakdown (right side)
    dims = health["dimensions"]
    dx = MARGIN + CONTENT_W // 2 + 40
    dw = CONTENT_W // 2 - 40
    dy = BODY_Y + 4

    for i, d in enumerate(dims):
        dot_map = {"HEALTHY": "\u25cf", "MODERATE": "\u25cf", "AT RISK": "\u25cf"}
        dot = dot_map.get(d["label"], "\u25cf")
        dim_line = f"{dot}  {d['name']}: {d['label']}"
        oid = f"{sid}_d{i}"
        _box(reqs, oid, sid, dx, dy, dw, 18, dim_line)
        _style(reqs, oid, 0, len(dim_line), bold=True, size=11, color=d["color"], font=FONT)

        det = f"     {d['detail']}  ({d['source']})"
        did = f"{sid}_dd{i}"
        _box(reqs, did, sid, dx, dy + 16, dw, 14, det)
        _style(reqs, did, 0, len(det), size=9, color=GRAY, font=FONT)

        dy += 44

    return idx + 1


def _engagement_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Engagement Breakdown")

    eng = report["engagement"]
    total = report["account"]["total_visitors"]

    _EG_KPI_H = 54
    _EG_GAP = 16.0
    kpy = BODY_Y + 8
    egw = (CONTENT_W - 2 * _EG_GAP) / 3
    tier_specs = [
        ("Active (7d)", eng["active_7d"], BLUE),
        ("Active (8–30d)", eng["active_30d"], BLUE),
        ("Dormant (30d+)", eng["dormant"], BLUE),
    ]
    for j, (lbl, cnt, ac) in enumerate(tier_specs):
        _kpi_metric_card(
            reqs, f"{sid}_ek{j}", sid, MARGIN + j * (egw + _EG_GAP), kpy, egw, _EG_KPI_H,
            lbl, f"{cnt}", accent=ac, value_pt=22,
        )
    content_top = kpy + _EG_KPI_H + 12

    charts = report.get("_charts")
    has_chart = False

    # Try to embed a donut chart for the tier distribution
    if charts and total > 0:
        try:
            from .charts import embed_chart, BRAND_SERIES_COLORS as _BSC
            active_7d = eng["active_7d"]
            active_30d = eng["active_30d"]
            dormant = eng["dormant"]
            donut_labels = ["Active (7d)", "Active (8–30d)", "Dormant (30d+)"]
            ss_id, chart_id = charts.add_pie_chart(
                title="User Engagement",
                labels=donut_labels,
                values=[active_7d, active_30d, dormant],
                donut=True,
            )
            legend_h = 22
            chart_w = 320
            chart_h = max(120, int(BODY_BOTTOM - content_top - 8 - legend_h))
            embed_chart(reqs, f"{sid}_donut", sid, ss_id, chart_id,
                        MARGIN, content_top, chart_w, chart_h)
            legend_entries = [(l, _BSC[i]) for i, l in enumerate(donut_labels) if i < len(_BSC)]
            _slide_chart_legend(reqs, sid, f"{sid}_dleg", MARGIN, content_top + chart_h + 4, legend_entries)
            has_chart = True
        except Exception as e:
            logger.warning("Chart embed failed for engagement slide: %s", e)

    # Text column: right of chart when present, else full width
    chart_used_w = 344 if has_chart else 0  # chart width + gap
    text_x = MARGIN + chart_used_w if has_chart else MARGIN
    text_w = CONTENT_W - chart_used_w if has_chart else CONTENT_W
    col_gap = 40
    col_w = (text_w - col_gap) // 2 if not has_chart else text_w

    tot_r = f"{total:,} tracked users"
    ry0 = content_top + 18
    active_roles = list(eng["role_active"].items())[:6]
    dormant_roles = list(eng["role_dormant"].items())[:6]

    if has_chart:
        _box(reqs, f"{sid}_tot", sid, text_x, content_top, text_w, 14, tot_r)
        _style(reqs, f"{sid}_tot", 0, len(tot_r), size=9, color=GRAY, font=FONT)
        ax = text_x
        ry = ry0
        if active_roles:
            ah = "Active Roles"
            _box(reqs, f"{sid}_ah", sid, ax, ry, col_w, 22, ah)
            _style(reqs, f"{sid}_ah", 0, len(ah), bold=True, size=14, color=BLUE, font=FONT)
            ry += 28
            for ri, (role, count) in enumerate(active_roles):
                if ry + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_ar{ri}", sid, ax, ry, col_w, 18, line)
                _style(reqs, f"{sid}_ar{ri}", 0, len(line), size=13, color=NAVY, font=FONT)
                _style(reqs, f"{sid}_ar{ri}", 0, len(f"{count:>4}"), bold=True, size=13, color=BLUE, font=FONT)
                ry += 22
        if dormant_roles and ry + 50 < BODY_BOTTOM:
            ry += 12
            dh = "Dormant Roles"
            _box(reqs, f"{sid}_dh", sid, ax, ry, col_w, 22, dh)
            _style(reqs, f"{sid}_dh", 0, len(dh), bold=True, size=14, color=GRAY, font=FONT)
            ry += 28
            for ri, (role, count) in enumerate(dormant_roles):
                if ry + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_dr{ri}", sid, ax, ry, col_w, 18, line)
                _style(reqs, f"{sid}_dr{ri}", 0, len(line), size=13, color=GRAY, font=FONT)
                _style(reqs, f"{sid}_dr{ri}", 0, len(f"{count:>4}"), bold=True, size=13)
                ry += 22
    else:
        _box(reqs, f"{sid}_tot", sid, MARGIN, content_top, CONTENT_W, 14, tot_r)
        _style(reqs, f"{sid}_tot", 0, len(tot_r), size=9, color=GRAY, font=FONT)
        lx = MARGIN
        rx = MARGIN + col_w + col_gap
        ry_l = ry0
        if active_roles:
            ah = "Active Roles"
            _box(reqs, f"{sid}_ah", sid, lx, ry_l, col_w, 22, ah)
            _style(reqs, f"{sid}_ah", 0, len(ah), bold=True, size=14, color=BLUE, font=FONT)
            ry_l += 28
            for ri, (role, count) in enumerate(active_roles):
                if ry_l + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_ar{ri}", sid, lx, ry_l, col_w, 18, line)
                _style(reqs, f"{sid}_ar{ri}", 0, len(line), size=13, color=NAVY, font=FONT)
                _style(reqs, f"{sid}_ar{ri}", 0, len(f"{count:>4}"), bold=True, size=13, color=BLUE, font=FONT)
                ry_l += 22
        ry_r = ry0
        if dormant_roles and ry_r + 50 < BODY_BOTTOM:
            dh = "Dormant Roles"
            _box(reqs, f"{sid}_dh", sid, rx, ry_r, col_w, 22, dh)
            _style(reqs, f"{sid}_dh", 0, len(dh), bold=True, size=14, color=GRAY, font=FONT)
            ry_r += 28
            for ri, (role, count) in enumerate(dormant_roles):
                if ry_r + 22 > BODY_BOTTOM:
                    break
                line = f"{count:>4}   {role}"
                _box(reqs, f"{sid}_dr{ri}", sid, rx, ry_r, col_w, 18, line)
                _style(reqs, f"{sid}_dr{ri}", 0, len(line), size=13, color=GRAY, font=FONT)
                _style(reqs, f"{sid}_dr{ri}", 0, len(f"{count:>4}"), bold=True, size=13)
                ry_r += 22

    return idx + 1


def _sites_slide(reqs, sid, report, idx):
    all_sites = report["sites"]
    if not all_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo site/list data")

    customer_prefix = report.get("account", {}).get("customer", "").strip()
    has_entity = any(s.get("entity") for s in all_sites)

    def _short_site(name: str) -> str:
        n = name
        if customer_prefix and n.lower().startswith(customer_prefix.lower()):
            n = n[len(customer_prefix):].lstrip(" -·")
        return n[:18] if len(n) > 18 else n

    # Compact body font; table *rendered* row height is ~font + default cell padding (~26 pt),
    # not 18 pt — using 18 for pagination caused overflow off the bottom of the slide.
    ROW_H = 26
    FONT_PT = 7
    table_top = BODY_Y

    if has_entity:
        headers = [
            "Site",
            "Entity",
            "Users",
            "Page views",
            "Feature clicks",
            "Events",
            "Minutes",
            "Last active",
        ]
        col_widths = [96, 72, 44, 56, 72, 48, 52, 64]
        end_col_start, end_col_end = 2, 6
    else:
        headers = [
            "Site",
            "Users",
            "Page views",
            "Feature clicks",
            "Events",
            "Minutes",
            "Last active",
        ]
        col_widths = [128, 44, 56, 72, 48, 52, 64]
        end_col_start, end_col_end = 1, 5

    num_cols = len(headers)
    rows_per_page = _table_rows_fit_span(
        y_top=table_top,
        y_bottom=BODY_BOTTOM,
        row_height_pt=ROW_H,
        reserved_table_rows=2,
        max_rows_cap=40,
    )
    show_total = len(all_sites) > 1
    num_pages = ((len(all_sites) + rows_per_page - 1) // rows_per_page) if rows_per_page else 1
    num_pages = _cap_page_count(num_pages)

    def _add_site_table(page_sid: str, table_sid: str, sites_chunk: list, add_total: bool) -> None:
        num_rows = 1 + len(sites_chunk) + (1 if add_total else 0)
        tbl_w = sum(col_widths)
        tbl_h = num_rows * ROW_H
        reqs.append({
            "createTable": {
                "objectId": table_sid,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(tbl_w, tbl_h),
                    "transform": _tf(MARGIN, table_top),
                },
                "rows": num_rows,
                "columns": num_cols,
            }
        })

        def _cell_loc(row, col):
            return {"rowIndex": row, "columnIndex": col}

        def _cell_text(row, col, text):
            reqs.append({"insertText": {"objectId": table_sid,
                         "cellLocation": _cell_loc(row, col),
                         "text": text, "insertionIndex": 0}})

        def _cell_style(row, col, text_len, bold=False, color=None, size=FONT_PT, align=None):
            if text_len > 0:
                s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}}
                f = ["fontSize"]
                if bold:
                    s["bold"] = True
                    f.append("bold")
                if color:
                    s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
                    f.append("foregroundColor")
                if FONT:
                    s["fontFamily"] = FONT
                    f.append("fontFamily")
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_sid, "cellLocation": _cell_loc(row, col),
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": s, "fields": ",".join(f),
                    }
                })
            if align:
                reqs.append({
                    "updateParagraphStyle": {
                        "objectId": table_sid, "cellLocation": _cell_loc(row, col),
                        "textRange": {"type": "ALL"},
                        "style": {"alignment": align},
                        "fields": "alignment",
                    }
                })

        def _cell_bg(row, col, color):
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_sid,
                    "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                    "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                    "fields": "tableCellBackgroundFill",
                }
            })

        _clean_table(reqs, table_sid, num_rows, num_cols)

        for ci, h in enumerate(headers):
            _cell_text(0, ci, h)
            _cell_style(0, ci, len(h), bold=True, color=GRAY, align="END" if end_col_start <= ci <= end_col_end else None)
            _cell_bg(0, ci, WHITE)

        for ri, s in enumerate(sites_chunk):
            row = ri + 1
            vals = [
                _short_site(s["sitename"]),
                (s.get("entity", "") or "")[:14] if has_entity else None,
                f'{s["visitors"]:,}',
                f'{s["page_views"]:,}',
                f'{s["feature_clicks"]:,}',
                f'{s["total_events"]:,}',
                f'{s["total_minutes"]:,}',
                (s.get("last_active") or "")[:10],
            ]
            if not has_entity:
                vals.pop(1)
            for ci, v in enumerate(vals):
                _cell_text(row, ci, v)
                _cell_style(row, ci, len(v), color=NAVY, align="END" if end_col_start <= ci <= end_col_end else None)
                _cell_bg(row, ci, WHITE)

        if add_total:
            total_row_idx = len(sites_chunk) + 1
            reqs.append({
                "updateTableBorderProperties": {
                    "objectId": table_sid,
                    "tableRange": {
                        "location": {"rowIndex": total_row_idx, "columnIndex": 0},
                        "rowSpan": 1, "columnSpan": num_cols,
                    },
                    "borderPosition": "TOP",
                    "tableBorderProperties": {
                        "tableBorderFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                        "weight": {"magnitude": 0.5, "unit": "PT"},
                        "dashStyle": "SOLID",
                    },
                    "fields": "tableBorderFill,weight,dashStyle",
                }
            })
            totals = [
                "Total",
                "" if has_entity else None,
                f'{sum(s["visitors"] for s in all_sites):,}',
                f'{sum(s["page_views"] for s in all_sites):,}',
                f'{sum(s["feature_clicks"] for s in all_sites):,}',
                f'{sum(s["total_events"] for s in all_sites):,}',
                f'{sum(s["total_minutes"] for s in all_sites):,}',
                "",
            ]
            if not has_entity:
                totals.pop(1)
            for ci, v in enumerate(totals):
                text = v if v is not None else ""
                if text or ci == 0:
                    _cell_text(total_row_idx, ci, text)
                _cell_style(total_row_idx, ci, len(text), bold=True, color=NAVY, align="END" if end_col_start <= ci <= end_col_end else None)
                _cell_bg(total_row_idx, ci, WHITE)

    for page in range(num_pages):
        page_sid = f"{sid}_p{page}" if num_pages > 1 else sid
        _slide(reqs, page_sid, idx + page)
        title = f"Site Comparison ({page + 1} of {num_pages})" if num_pages > 1 else "Site Comparison"
        _slide_title(reqs, page_sid, title)

        start = page * rows_per_page
        chunk = all_sites[start : start + rows_per_page]
        add_total = show_total and (page == num_pages - 1)
        _add_site_table(page_sid, f"{page_sid}_table", chunk, add_total)

    slide_oids = [f"{sid}_p{i}" for i in range(num_pages)] if num_pages > 1 else [sid]
    return idx + num_pages, slide_oids


def _features_slide(reqs, sid, report, idx):
    pages = report["top_pages"]
    features = report["top_features"]
    if not pages and not features:
        return _missing_data_slide(reqs, sid, report, idx, "top pages / feature adoption data")

    font_body = 12
    font_header = 14
    col_gap = 24
    col_w = (CONTENT_W - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_w + col_gap
    _ins = report.get("feature_adoption_insights") or {}
    _ins_text = (_ins.get("narrative") or "").strip() if isinstance(_ins, dict) else ""
    _ins_band = 74  # reserved height for usage-pattern footnote (pt)
    # One vertical budget for all pages: tightest is slide 1 when footnote is shown.
    _tight_bottom = BODY_BOTTOM - (_ins_band if _ins_text else 0)
    max_items = _list_data_rows_fit_span(
        y_top=BODY_Y,
        y_bottom=_tight_bottom,
        font_body_pt=font_body,
        reserved_header_lines=1,
        max_rows_cap=30,
    )

    def _render_column(page_sid, prefix, col_title, items, name_key, events_key, events_suffix, start_rank: int, box_h: int):
        lines = [col_title]
        slice_items = items[start_rank : start_rank + max_items]
        for j, it in enumerate(slice_items, start=start_rank + 1):
            nm = (it[name_key] or "")[:32]
            if len(it.get(name_key) or "") > 32:
                nm = nm.rstrip() + "…"
            lines.append(f"  {j}. {nm}  ({it[events_key]:,} {events_suffix})")
        if not slice_items and start_rank == 0:
            lines.append("  No data")
        text = "\n".join(lines)
        oid = f"{page_sid}_{prefix}"
        _box(reqs, oid, page_sid, left_x if prefix == "pg" else right_x, BODY_Y, col_w, box_h, text)
        _style(reqs, oid, 0, len(text), size=font_body, color=NAVY, font=FONT)
        _style(reqs, oid, 0, len(col_title), bold=True, size=font_header, color=BLUE)

    n_pg = (len(pages) + max_items - 1) // max_items if pages else 0
    n_ft = (len(features) + max_items - 1) // max_items if features else 0
    num_pages = _cap_page_count(max(n_pg, n_ft, 1))
    oids: list[str] = []
    for p in range(num_pages):
        page_sid = f"{sid}_p{p}" if num_pages > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + p)
        st = "Feature Adoption" if num_pages == 1 else f"Feature Adoption ({p + 1} of {num_pages})"
        _slide_title(reqs, page_sid, st)
        foot = _ins_text if (p == 0 and _ins_text) else ""
        col_bottom = BODY_BOTTOM - (_ins_band if foot else 0)
        box_h = col_bottom - BODY_Y
        _render_column(page_sid, "pg", "Top Pages", pages, "name", "events", "events", p * max_items, box_h)
        _render_column(page_sid, "ft", "Top Features", features, "name", "events", "clicks", p * max_items, box_h)
        if foot:
            ins_oid = f"{page_sid}_usagepat"
            _wrap_box(reqs, ins_oid, page_sid, MARGIN, col_bottom, CONTENT_W, _ins_band - 4, foot)
            _style(reqs, ins_oid, 0, len(foot), size=10, color=GRAY, font=FONT)
    return idx + num_pages, oids


def _champions_slide(reqs, sid, report, idx):
    all_champions = report["champions"]
    all_at_risk = report["at_risk_users"]
    if not all_champions and not all_at_risk:
        return _missing_data_slide(reqs, sid, report, idx, "champion / at-risk user data")

    _CHAMPIONS_COL_MAX = 5
    _AT_RISK_COL_MAX = 5

    def _days_inactive(u: dict) -> float:
        d = u.get("days_inactive")
        return float(d) if d is not None else 999.0

    ch_sorted = sorted(all_champions, key=_days_inactive)[:_CHAMPIONS_COL_MAX]
    ar_sorted = sorted(all_at_risk, key=_days_inactive)[:_AT_RISK_COL_MAX]

    USER_H = 38
    col_gap = 30
    col_w = (CONTENT_W - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_w + col_gap

    def _render_users(page_sid, users, x, label, label_color, detail_fn, prefix, start_i: int):
        y = BODY_Y
        _box(reqs, f"{page_sid}_{prefix}h", page_sid, x, y, col_w, 22, label)
        _style(reqs, f"{page_sid}_{prefix}h", 0, len(label), bold=True, size=14, color=label_color, font=FONT)
        y += 28

        if not users and start_i == 0:
            empty = "No active users" if prefix == "c" else "All users active!"
            _box(reqs, f"{page_sid}_{prefix}e", page_sid, x, y, col_w, 20, empty)
            _style(reqs, f"{page_sid}_{prefix}e", 0, len(empty), size=12, color=GRAY, font=FONT, italic=True)
            return

        for ui, u in enumerate(users):
            email = u["email"] or "unknown"
            if len(email) > 28:
                email = email[:25] + "..."
            detail = detail_fn(u)
            _box(reqs, f"{page_sid}_{prefix}{start_i + ui}", page_sid, x, y, col_w, 18, email)
            _style(reqs, f"{page_sid}_{prefix}{start_i + ui}", 0, len(email), bold=True, size=12, color=NAVY, font=FONT)
            _box(reqs, f"{page_sid}_{prefix}d{start_i + ui}", page_sid, x + 8, y + 18, col_w - 8, 16, detail)
            _style(reqs, f"{page_sid}_{prefix}d{start_i + ui}", 0, len(detail), size=10, color=GRAY, font=FONT)
            y += USER_H

    def _champ_detail(u):
        return f"{u['role']}  ·  last seen {u['last_visit']}"

    def _risk_detail(u):
        d = f"{int(u['days_inactive'])}d ago" if u["days_inactive"] < 999 else "never"
        return f"{u['role']}  ·  {d}"

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Champions & At-Risk Users")
    _render_users(sid, ch_sorted, left_x, "Champions", BLUE, _champ_detail, "c", 0)
    _render_users(sid, ar_sorted, right_x, "At Risk  (2 wk – 6 mo inactive)", GRAY, _risk_detail, "r", 0)
    return idx + 1, [sid]


def _benchmarks_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Peer Benchmarks")

    bench = report["benchmarks"]
    acct = report["account"]
    cust_rate = bench["customer_active_rate"]
    all_med = bench["peer_median_rate"]
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    cohort_name = bench.get("cohort_name", "")
    use_cohort = cohort_med is not None and cohort_n >= benchmarks_min_peers_for_cohort_median()
    med_rate = cohort_med if use_cohort else all_med
    delta = cust_rate - med_rate

    row_y = BODY_Y + 8
    card_h = 58
    col_gap = 18.0
    n_cards = 3 if use_cohort else 2
    card_w = (CONTENT_W - (n_cards - 1) * col_gap) / n_cards

    _kpi_metric_card(
        reqs, f"{sid}_k0", sid, MARGIN, row_y, card_w, card_h,
        "Weekly active rate (this account)", f"{cust_rate}%", accent=BLUE, value_pt=22,
    )

    if use_cohort:
        med_lbl = _truncate_kpi_card_label(f"{cohort_name} median ({cohort_n} accounts)")
    else:
        med_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
    _kpi_metric_card(
        reqs, f"{sid}_k1", sid, MARGIN + card_w + col_gap, row_y, card_w, card_h,
        med_lbl, f"{med_rate}%", accent=BLUE, value_pt=22,
    )

    if use_cohort:
        all_lbl = _truncate_kpi_card_label(f"All-customer median ({bench['peer_count']} accounts)")
        _kpi_metric_card(
            reqs, f"{sid}_k2", sid, MARGIN + 2 * (card_w + col_gap), row_y, card_w, card_h,
            all_lbl, f"{all_med}%", accent=BLUE, value_pt=22,
        )

    # Context (narrative — outside KPI cards; see SLIDE_DESIGN_STANDARDS KPI boxes for app-built slides)
    peer_label = cohort_name if use_cohort else "peer"
    lines = [
        f"Delta: {'+' if delta >= 0 else ''}{delta:.0f} percentage points vs {peer_label} median",
        f"Account size: {acct['total_visitors']} users across {acct['total_sites']} sites",
        "",
    ]
    if delta > 15:
        lines.append(f"Engagement significantly exceeds {peer_label} average.")
        lines.append("Strong candidate for case study, reference, or expansion.")
    elif delta > 0:
        lines.append(f"Performing above {peer_label} average.")
        lines.append("Continue strategy; watch for expansion signals.")
    elif delta > -10:
        lines.append(f"Near the {peer_label} average.")
        lines.append("Monitor for downward trend; proactive outreach recommended.")
    else:
        lines.append(f"Significantly below {peer_label} average.")
        lines.append("Recommend re-engagement, executive check-in, training refresh.")

    ctx = "\n".join(lines)
    ctx_y = row_y + card_h + 16
    ctx_h = max(96.0, BODY_BOTTOM - ctx_y - 4)
    _box(reqs, f"{sid}_ctx", sid, MARGIN, ctx_y, CONTENT_W, ctx_h, ctx)
    _style(reqs, f"{sid}_ctx", 0, len(ctx), size=11, color=BLUE, font=FONT)

    return idx + 1


def _exports_slide(reqs, sid, report, idx):
    exports = report.get("exports", report)
    by_feature = exports.get("by_feature", [])
    top_exporters = exports.get("top_exporters", [])
    total = exports.get("total_exports", 0)

    if not by_feature and total == 0:
        return _missing_data_slide(reqs, sid, report, idx, "export / benchmark data")

    per_user = exports.get("exports_per_active_user", 0)
    active = exports.get("active_users", 0)
    header = f"{total:,} exports  ·  {per_user}/active user  ·  {active} active users"
    _exp_list_top = BODY_Y + 24
    # Use full body band so feature and exporter columns share one line budget (same box height).
    _exp_list_bottom = min(_exp_list_top + 270, BODY_BOTTOM - 4)
    _exp_list_h = max(120.0, float(_exp_list_bottom) - float(_exp_list_top))
    line_budget = _list_data_rows_fit_span(
        y_top=_exp_list_top,
        y_bottom=_exp_list_top + _exp_list_h,
        font_body_pt=10,
        reserved_header_lines=1,
        max_rows_cap=40,
    )
    # By Feature: one line per row. Top Exporters: two lines per user (email + detail).
    max_features = line_budget
    max_exporters = max(1, line_budget // 2)
    n_fp = (len(by_feature) + max_features - 1) // max_features if by_feature else 0
    n_ep = (len(top_exporters) + max_exporters - 1) // max_exporters if top_exporters else 0
    num_pages = _cap_page_count(max(n_fp, n_ep, 1))
    oids: list[str] = []
    for p in range(num_pages):
        page_sid = f"{sid}_p{p}" if num_pages > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + p)
        st = "Export Behavior" if num_pages == 1 else f"Export Behavior ({p + 1} of {num_pages})"
        _slide_title(reqs, page_sid, st)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=GRAY, font=FONT)
        fl = ["By Feature"]
        feat_slice = by_feature[p * max_features : (p + 1) * max_features]
        start_i = p * max_features
        for j, f in enumerate(feat_slice, start=start_i + 1):
            name = f["feature"][:36] if len(f["feature"]) > 36 else f["feature"]
            fl.append(f"  {j}. {name}  ({f['exports']:,})")
        if not feat_slice and p == 0 and not by_feature:
            fl.append("  No export data")
        ft = "\n".join(fl)
        _box(reqs, f"{page_sid}_bf", page_sid, MARGIN, BODY_Y + 24, 340, 270, ft)
        _style(reqs, f"{page_sid}_bf", 0, len(ft), size=10, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid}_bf", 0, len("By Feature"), bold=True, size=11, color=BLUE)
        el = ["Top Exporters"]
        exp_slice = top_exporters[p * max_exporters : (p + 1) * max_exporters]
        for u in exp_slice:
            email = u["email"] or "unknown"
            if len(email) > 32:
                email = email[:29] + "..."
            el.append(f"  {email}")
            el.append(f"    {u['role']}  ·  {u['exports']:,} exports")
        if not exp_slice and p == 0 and not top_exporters:
            el.append("  No export users")
        et = "\n".join(el)
        _box(reqs, f"{page_sid}_te", page_sid, 400, BODY_Y + 24, 280, 270, et)
        _style(reqs, f"{page_sid}_te", 0, len(et), size=10, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid}_te", 0, len("Top Exporters"), bold=True, size=11, color=BLUE)
    return idx + num_pages, oids


def _depth_slide(reqs, sid, report, idx):
    depth = report.get("depth", report)
    breakdown = depth.get("breakdown", [])
    if not breakdown:
        return _missing_data_slide(reqs, sid, report, idx, "depth-of-use breakdown data")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Behavioral Depth")

    write_ratio = depth.get("write_ratio", 0)
    total = depth.get("total_feature_events", 0)
    active = depth.get("active_users", 0)

    _DP_KPI_H = 54
    _DP_GAP = 16.0
    _DP_CHART_GAP = 16.0
    dpy = BODY_Y + 8
    dpw = (CONTENT_W - 2 * _DP_GAP) / 3
    _kpi_metric_card(
        reqs, f"{sid}_dk0", sid, MARGIN, dpy, dpw, _DP_KPI_H,
        "Feature interactions", f"{total:,}", accent=BLUE, value_pt=20,
    )
    _kpi_metric_card(
        reqs, f"{sid}_dk1", sid, MARGIN + dpw + _DP_GAP, dpy, dpw, _DP_KPI_H,
        "Active users", f"{active}", accent=BLUE, value_pt=20,
    )
    _kpi_metric_card(
        reqs, f"{sid}_dk2", sid, MARGIN + 2 * (dpw + _DP_GAP), dpy, dpw, _DP_KPI_H,
        "Write ratio", f"{write_ratio}%", accent=BLUE, value_pt=20,
    )
    chart_top = dpy + _DP_KPI_H + _DP_CHART_GAP

    charts = report.get("_charts")
    read_e = depth.get("read_events", 0)
    write_e = depth.get("write_events", 0)
    collab_e = depth.get("collab_events", 0)

    if charts:
        try:
            from .charts import embed_chart
            bottom_pad = 16
            chart_h = BODY_BOTTOM - chart_top - bottom_pad

            # Stacked bar: top categories by read/write/collab
            top = breakdown[:8]
            labels = [b["category"] for b in top]
            read_vals = [b.get("read", 0) for b in top]
            write_vals = [b.get("write", 0) for b in top]
            collab_vals = [b.get("collab", 0) for b in top]
            has_rwc = any(v > 0 for v in read_vals + write_vals + collab_vals)
            pie_ok = read_e + write_e + collab_e > 0

            if has_rwc and pie_ok:
                from .charts import BRAND_SERIES_COLORS as _BSC
                gap = 8.0
                legend_h = 22
                left_w = (CONTENT_W - gap) * 0.58
                right_w = CONTENT_W - gap - left_w
                vis_chart_h = chart_h - legend_h
                rwc_labels = ["Read", "Write", "Collab"]
                ss_id, chart_id = charts.add_bar_chart(
                    title="Feature Category Depth",
                    labels=labels,
                    series={"Read": read_vals, "Write": write_vals, "Collab": collab_vals},
                    horizontal=True,
                    stacked=True,
                    suppress_legend=True,
                )
                embed_chart(
                    reqs, f"{sid}_chart", sid, ss_id, chart_id,
                    MARGIN, chart_top, left_w, vis_chart_h,
                )
                legend_entries = [(l, _BSC[i]) for i, l in enumerate(rwc_labels) if i < len(_BSC)]
                _slide_chart_legend(reqs, sid, f"{sid}_bleg", MARGIN, chart_top + vis_chart_h + 4, legend_entries)

                ss_id2, pie_id = charts.add_pie_chart(
                    title="Read / Write / Collab",
                    labels=rwc_labels,
                    values=[read_e, write_e, collab_e],
                    donut=True,
                )
                pie_x = MARGIN + left_w + gap
                embed_chart(
                    reqs, f"{sid}_pie", sid, ss_id2, pie_id,
                    pie_x, chart_top, right_w, vis_chart_h,
                )
            elif has_rwc:
                from .charts import BRAND_SERIES_COLORS as _BSC
                legend_h = 22
                bx, by, bw, bh = _single_embedded_chart_layout(
                    y_top=chart_top, bottom_pad=bottom_pad + legend_h, pie_or_donut=False,
                )
                rwc_labels = ["Read", "Write", "Collab"]
                ss_id, chart_id = charts.add_bar_chart(
                    title="Feature Category Depth",
                    labels=labels,
                    series={"Read": read_vals, "Write": write_vals, "Collab": collab_vals},
                    horizontal=True,
                    stacked=True,
                    suppress_legend=True,
                )
                embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id, bx, by, bw, bh)
                legend_entries = [(l, _BSC[i]) for i, l in enumerate(rwc_labels) if i < len(_BSC)]
                _slide_chart_legend(reqs, sid, f"{sid}_bleg", bx, by + bh + 4, legend_entries)
            elif pie_ok:
                from .charts import BRAND_SERIES_COLORS as _BSC
                legend_h = 22
                px, py, pw, ph = _single_embedded_chart_layout(
                    y_top=chart_top, bottom_pad=bottom_pad + legend_h, pie_or_donut=True,
                )
                pie_labels = ["Read", "Write", "Collab"]
                ss_id2, pie_id = charts.add_pie_chart(
                    title="Read / Write / Collab",
                    labels=pie_labels,
                    values=[read_e, write_e, collab_e],
                    donut=True,
                )
                embed_chart(reqs, f"{sid}_pie", sid, ss_id2, pie_id, px, py, pw, ph)
                legend_entries = [(l, _BSC[i]) for i, l in enumerate(pie_labels) if i < len(_BSC)]
                _slide_chart_legend(reqs, sid, f"{sid}_pleg", px, py + ph + 4, legend_entries)
        except Exception as e:
            logger.warning("Chart embed failed for depth slide: %s", e)

    return idx + 1


def _kei_slide(reqs, sid, report, idx):
    kei = report.get("kei", report)
    total_q = kei.get("total_queries", 0)

    _slide(reqs, sid, idx)
    title = "Kei AI Adoption" if total_q > 0 else "Kei AI Adoption — No Usage"
    _slide_title(reqs, sid, title)

    unique = kei.get("unique_users", 0)
    adoption = kei.get("adoption_rate", 0)
    exec_users = kei.get("executive_users", 0)
    exec_queries = kei.get("executive_queries", 0)

    _KEI_KPI_H = 58
    _KEI_GAP = 18.0
    krow = BODY_Y + 8
    kcw = (CONTENT_W - 2 * _KEI_GAP) / 3
    _kpi_metric_card(
        reqs, f"{sid}_k0", sid, MARGIN, krow, kcw, _KEI_KPI_H,
        "Total queries", f"{total_q:,}", accent=BLUE, value_pt=22,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k1", sid, MARGIN + kcw + _KEI_GAP, krow, kcw, _KEI_KPI_H,
        "Adoption rate", f"{adoption}%", accent=BLUE, value_pt=22,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k2", sid, MARGIN + 2 * (kcw + _KEI_GAP), krow, kcw, _KEI_KPI_H,
        "Users with queries", f"{unique}", accent=BLUE, value_pt=22,
    )

    exec_y = krow + _KEI_KPI_H + 10
    # Executive highlight pill
    if exec_users > 0:
        exec_text = f"  {exec_users} executives ({exec_queries:,} queries)  "
        _pill(reqs, f"{sid}_exec", sid, MARGIN, exec_y, 260, 22, exec_text, BLUE, WHITE)
    else:
        exec_text = "  No executive Kei usage detected  "
        _pill(reqs, f"{sid}_exec", sid, MARGIN, exec_y, 260, 22, exec_text, GRAY, WHITE)

    # User list
    users = kei.get("users", [])
    lines = ["Kei Users"]
    users_top = exec_y + 28 + 8
    for u in users[:8]:
        email = u.get("email", "unknown")
        if len(email) > 30:
            email = email[:27] + "..."
        role = u.get("role", "")
        exec_flag = " *" if u.get("is_executive") else ""
        lines.append(f"  {email}")
        lines.append(f"    {role}{exec_flag}  ·  {u.get('queries', 0):,} queries")
    if not users:
        lines.append("  No Kei usage in this period")
    text = "\n".join(lines)
    users_h = max(120.0, BODY_BOTTOM - users_top - 4)
    _box(reqs, f"{sid}_users", sid, MARGIN, users_top, CONTENT_W, users_h, text)
    _style(reqs, f"{sid}_users", 0, len(text), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_users", 0, len("Kei Users"), bold=True, size=11, color=BLUE)

    return idx + 1


def _guides_no_usage_slide(reqs, sid, report, idx, guides: dict[str, Any]) -> int:
    """Guide engagement succeeded but zero events — explicit signal, not missing data."""
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Guide Engagement")

    days = guides.get("days")
    tv = int(guides.get("total_visitors") or 0)
    scope_parts = [f"{tv:,} tracked visitors"]
    if days is not None:
        scope_parts.append(f"{days}-day lookback")
    scope = "  ·  ".join(scope_parts)
    _box(reqs, f"{sid}_scope", sid, MARGIN, BODY_Y, CONTENT_W, 18, scope)
    _style(reqs, f"{sid}_scope", 0, len(scope), size=10, color=GRAY, font=FONT)

    headline = "No usage"
    _box(reqs, f"{sid}_nu", sid, MARGIN, BODY_Y + 32, CONTENT_W, 36, headline)
    _style(reqs, f"{sid}_nu", 0, len(headline), bold=True, size=22, color=NAVY, font=FONT)

    detail = (
        "No in-app guide events (views, continue/next, or dismiss) were recorded for this "
        "customer in this period — an adoption signal worth reviewing with the account team."
    )
    _wrap_box(reqs, f"{sid}_nu_d", sid, MARGIN, BODY_Y + 76, CONTENT_W, 120, detail)
    _style(reqs, f"{sid}_nu_d", 0, len(detail), size=11, color=NAVY, font=FONT)

    return idx + 1


def _guides_slide(reqs, sid, report, idx):
    guides = report.get("guides")
    if not isinstance(guides, dict):
        return _missing_data_slide(reqs, sid, report, idx, "guide engagement data")
    err = guides.get("error")
    if err:
        return _missing_data_slide(reqs, sid, report, idx, f"guide engagement: {err}")

    total_events = int(guides.get("total_guide_events") or 0)
    if total_events == 0:
        return _guides_no_usage_slide(reqs, sid, report, idx, guides)

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Guide Engagement")

    seen = guides.get("seen", 0)
    advanced = guides.get("advanced", 0)
    dismissed = guides.get("dismissed", 0)
    reach = guides.get("guide_reach", 0)
    dismiss_rate = guides.get("dismiss_rate", 0)
    advance_rate = guides.get("advance_rate", 0)

    metrics = (
        f"{seen:,} guide views (Pendo)  ·  {reach}% of tracked users saw at least one guide\n"
        f"{advance_rate}% of views included a continue (next step)  ·  "
        f"{dismiss_rate}% of views included a dismiss"
    )
    _box(reqs, f"{sid}_met", sid, MARGIN, BODY_Y, CONTENT_W, 34, metrics)
    _style(reqs, f"{sid}_met", 0, len(metrics), size=10, color=GRAY, font=FONT)

    # Continue vs dismiss: bar is split of those two event types only (many views have neither).
    bar_y = BODY_Y + 42
    total_responses = advanced + dismissed
    if total_responses > 0:
        adv_w = int(advanced / total_responses * 400)
        dis_w = int(dismissed / total_responses * 400)
        _bar_rect(reqs, f"{sid}_adv", sid, MARGIN, bar_y, max(adv_w, 4), 18, BLUE)
        _bar_rect(reqs, f"{sid}_dis", sid, MARGIN + adv_w, bar_y, max(dis_w, 4), 18, GRAY)
        alab = f"Continue / next step ({advanced:,})"
        _box(reqs, f"{sid}_alab", sid, MARGIN, bar_y + 20, 220, 14, alab)
        _style(reqs, f"{sid}_alab", 0, len(alab), size=8, color=BLUE, font=FONT)
        dlab = f"Dismissed ({dismissed:,})"
        _box(reqs, f"{sid}_dlab", sid, MARGIN + adv_w, bar_y + 20, 220, 14, dlab)
        _style(reqs, f"{sid}_dlab", 0, len(dlab), size=8, color=GRAY, font=FONT)
        bar_note = (
            "Bar = share of continue vs dismiss events only; other guide views did not continue or dismiss."
        )
        _box(reqs, f"{sid}_bnote", sid, MARGIN, bar_y + 36, CONTENT_W, 22, bar_note)
        _style(reqs, f"{sid}_bnote", 0, len(bar_note), size=7, color=GRAY, font=FONT)
        bar_y += 62

    # Top guides — one bullet per guide; legend explains Pendo event meanings.
    top_guides = guides.get("top_guides", [])
    sec_title = "Most active guides"
    sec_legend = (
        "Each bullet: views = times the guide was shown; continue = user went to the next step; "
        "dismiss = user closed the guide without continuing."
    )
    bullet_lines: list[str] = []
    for g in top_guides[:6]:
        name = str(g.get("guide") or "")
        if len(name) > 52:
            name = name[:49] + "..."
        bullet_lines.append(
            f"• {name}: {g['seen']} views, {g['advanced']} continue, {g['dismissed']} dismiss"
        )
    if not bullet_lines:
        bullet_lines.append("• No guide interactions in this period.")
    body = "\n".join(bullet_lines)
    text = f"{sec_title}\n{sec_legend}\n\n{body}"
    _box(reqs, f"{sid}_guides", sid, MARGIN, bar_y + 4, CONTENT_W, 220, text)
    _style(reqs, f"{sid}_guides", 0, len(text), size=10, color=NAVY, font=FONT)
    _len_title = len(sec_title)
    _style(reqs, f"{sid}_guides", 0, _len_title, bold=True, size=11, color=BLUE, font=FONT)
    _leg_start = _len_title + 1
    _leg_end = _leg_start + len(sec_legend)
    _style(reqs, f"{sid}_guides", _leg_start, _leg_end, size=8, color=GRAY, font=FONT)

    return idx + 1


def _custom_slide(reqs, sid, report, idx):
    """Flexible slide renderer for agent-composed content.

    Expects data with:
        title: str
        sections: list of {header: str, body: str}
    """
    title = report.get("title", "")
    sections = report.get("sections", [])
    if not title and not sections:
        return _missing_data_slide(reqs, sid, report, idx, "deck title / section list")

    _slide(reqs, sid, idx)
    if title:
        _slide_title(reqs, sid, title)

    y = BODY_Y
    col_w = CONTENT_W
    if len(sections) == 2:
        col_w = 300
    elif len(sections) >= 3:
        col_w = 195

    for i, sec in enumerate(sections[:3]):
        header = sec.get("header", "")
        body = sec.get("body", "")
        x = MARGIN + i * (col_w + 16)

        if header:
            _box(reqs, f"{sid}_h{i}", sid, x, y, col_w, 18, header)
            _style(reqs, f"{sid}_h{i}", 0, len(header), bold=True, size=11, color=BLUE, font=FONT)

        if body:
            body_y = y + (22 if header else 0)
            _box(reqs, f"{sid}_b{i}", sid, x, body_y, col_w, 280, body)
            _style(reqs, f"{sid}_b{i}", 0, len(body), size=10, color=NAVY, font=FONT)

    return idx + 1


def _jira_slide(reqs, sid, report, idx):
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(reqs, sid, report, idx, "Jira support ticket data")
    jira_base = jira.get("base_url", "")

    total = jira["total_issues"]
    open_n = jira["open_issues"]
    resolved = jira["resolved_issues"]
    esc = jira["escalated"]
    bugs = jira["open_bugs"]
    days = jira.get("days", 90)

    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    date_range = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
    header = (
        f"{total} HELP tickets  ·  {date_range}  ·  {open_n} open  ·  {resolved} resolved  ·  "
        f"{esc} escalated  ·  {bugs} open bugs"
    )

    sla_lines = []
    ttfr = jira.get("ttfr", {})
    if ttfr.get("measured", 0) > 0:
        parts = [f"First Response:  median {ttfr['median']}  ·  avg {ttfr['avg']}"]
        if ttfr.get("breached"):
            parts.append(f"  ·  {ttfr['breached']} breach{'es' if ttfr['breached'] != 1 else ''}")
        if ttfr.get("waiting"):
            parts.append(f"  ·  {ttfr['waiting']} awaiting")
        sla_lines.append("".join(parts))
    ttr = jira.get("ttr", {})
    if ttr.get("measured", 0) > 0:
        parts = [f"Resolution:  median {ttr['median']}  ·  avg {ttr['avg']}"]
        if ttr.get("breached"):
            parts.append(f"  ·  {ttr['breached']} breach{'es' if ttr['breached'] != 1 else ''}")
        if ttr.get("waiting"):
            parts.append(f"  ·  {ttr['waiting']} unresolved")
        sla_lines.append("".join(parts))
    if sla_lines:
        sla_text = "\n".join(sla_lines)
        body_offset = 22 + 12 * len(sla_lines)
    else:
        sla_text = ""
        body_offset = 28

    status_items = list(jira.get("by_status", {}).items())
    sum_status = sum(c for _, c in status_items)
    sum_priority = sum(c for _, c in jira.get("by_priority", {}).items())
    status_lines = [f"{c:>4}  {s}" for s, c in status_items]
    prio_short = {"Blocker: The platform is completely down": "Blocker",
                  "Critical: Significant operational impact": "Critical",
                  "Major: Workaround available, not essential": "Major",
                  "Minor: Impairs non-essential functionality": "Minor"}
    prio_items = list(jira.get("by_priority", {}).items())
    prio_lines = [f"{c:>4}  {prio_short.get(p, p[:20])}" for p, c in prio_items]

    recent_all = list(jira.get("recent_issues", []))
    esc_issues = list(jira.get("escalated_issues", []))
    eng = jira.get("engineering", {})
    eng_open = list(eng.get("open", []))
    eng_closed = list(eng.get("recent_closed", []))
    eng_hdr = (
        f"Engineering Pipeline  ({eng.get('open_count', len(eng_open))} open · "
        f"{eng.get('closed_count', len(eng_closed))} closed)"
    )

    col_gap = 20
    left_w = (CONTENT_W - col_gap) // 2
    right_w = CONTENT_W - left_w - col_gap
    max_y = BODY_BOTTOM
    line_h = 12
    sec_title_h = 16

    def _pack_lines(lines: list[str], budget: float) -> tuple[list[str], list[str], float]:
        if budget < sec_title_h + line_h:
            return [], lines, 0.0
        used = float(sec_title_h)
        take: list[str] = []
        rest = list(lines)
        while rest and used + line_h <= budget:
            take.append(rest.pop(0))
            used += line_h
        return take, rest, used

    def _continuation_pages(
        sections: list[tuple[str, list[str]]],
        start_idx: int,
        total_pages: int,
        body_top_y: float,
        *,
        max_slide_index_exclusive: int | None = None,
        reconcile_header_total: int | None = None,
    ) -> tuple[int, list[str]]:
        oids_extra: list[str] = []
        cont_y0 = body_top_y
        per_page_lines = max(1, int((max_y - cont_y0 - 8) // line_h))
        flat: list[tuple[str, str]] = []
        if reconcile_header_total is not None and start_idx >= 1:
            flat.append((
                "n",
                f"Together with slide 1, the breakdowns below account for all "
                f"{reconcile_header_total} tickets in the header.",
            ))
        for sec_title, slines in sections:
            if not slines:
                continue
            flat.append(("h", sec_title))
            for ln in slines:
                flat.append(("l", ln))
        pos = 0
        page_num = start_idx
        while pos < len(flat) and (max_slide_index_exclusive is None or page_num < max_slide_index_exclusive):
            page_sid = f"{sid}_p{page_num}"
            oids_extra.append(page_sid)
            _slide(reqs, page_sid, idx + page_num)
            _bg(reqs, page_sid, WHITE)
            ttl = "Support Summary" if total_pages == 1 else f"Support Summary ({page_num + 1} of {total_pages})"
            _slide_title(reqs, page_sid, ttl)
            _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
            _style(reqs, f"{page_sid}_hdr", 0, len(header), size=11, color=NAVY, font=FONT, bold=True)
            y = cont_y0
            used_lines = 0
            while pos < len(flat) and used_lines < per_page_lines:
                kind, text = flat[pos]
                if kind == "h":
                    _box(reqs, f"{page_sid}_h{pos}", page_sid, MARGIN, y, CONTENT_W, sec_title_h, text)
                    _style(reqs, f"{page_sid}_h{pos}", 0, len(text), bold=True, size=10, color=BLUE, font=FONT)
                    y += sec_title_h
                    used_lines += 1
                elif kind == "n":
                    _box(reqs, f"{page_sid}_n{pos}", page_sid, MARGIN, y, CONTENT_W, line_h * 2 + 4, text)
                    _style(reqs, f"{page_sid}_n{pos}", 0, len(text), size=8, color=GRAY, font=FONT)
                    y += line_h * 2 + 6
                    used_lines += 2
                else:
                    _box(reqs, f"{page_sid}_l{pos}", page_sid, MARGIN, y, CONTENT_W, line_h, text)
                    _style(reqs, f"{page_sid}_l{pos}", 0, len(text), size=8, color=NAVY, font=MONO)
                    if jira_base and text and text.strip():
                        key = text.split()[0] if text.split() else ""
                        if key and "-" in key:
                            lk = len(key)
                            _style(reqs, f"{page_sid}_l{pos}", 0, lk, bold=True, size=8, color=BLUE, font=MONO,
                                   link=f"{jira_base}/browse/{key}")
                    y += line_h
                    used_lines += 1
                pos += 1
            page_num += 1
        return page_num, oids_extra

    # ── Page 1: pack two columns; push overflow into continuation sections ──
    status_rest: list[str] = []
    prio_rest: list[str] = []
    recent_rest: list = []
    esc_rest: list = []
    eng_open_rest: list = []
    eng_closed_rest: list = []
    oids: list[str] = []

    body_top = BODY_Y + body_offset
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap
    reserve_bottom = 36
    summary_budget = max(max_y - body_top - reserve_bottom - 140, sec_title_h + line_h)

    st_take, status_rest, st_used = _pack_lines(status_lines, summary_budget)
    pr_budget = max(summary_budget - st_used - 4, sec_title_h + line_h)
    pr_take, prio_rest, pr_used = _pack_lines(prio_lines, pr_budget)

    st_footer_h = 0
    if st_take and status_rest:
        st_footer_h = line_h * 3
    elif st_take and sum_status != total:
        st_footer_h = line_h * 2
    pr_footer_h = 0
    if pr_take and prio_rest:
        pr_footer_h = line_h * 3
    elif pr_take and sum_priority != total:
        pr_footer_h = line_h * 2

    left_y = body_top + st_used + 4 + pr_used + 4 + st_footer_h + pr_footer_h
    recent_take: list = []
    if recent_all:
        avail_recent = max(int((max_y - left_y) // line_h) - 1, 0)
        recent_take = recent_all[:avail_recent]
        recent_rest = recent_all[avail_recent:]

    right_y = body_top
    esc_take = []
    if esc_issues or esc > 0:
        esc_line_budget = max(int((max_y - right_y - reserve_bottom) // line_h) - 1, 0)
        if esc_line_budget <= 0 and esc_issues:
            esc_rest = list(esc_issues)
        else:
            esc_take = esc_issues[:esc_line_budget]
            esc_rest = esc_issues[esc_line_budget:]

    show_esc_block_p1 = bool(esc_take) or (esc > 0 and not esc_rest)
    right_y_eng = body_top
    if show_esc_block_p1:
        right_y_eng += line_h * (len(esc_take) + 1) + 10

    eng_open_take: list = []
    eng_closed_take: list = []
    eng_open_rest: list = []
    eng_closed_rest: list = []
    if eng_open or eng_closed:
        avail_eng = max(int((max_y - right_y_eng) // line_h) - 2, 1)
        eng_open_take = eng_open[:avail_eng]
        eng_open_rest = eng_open[len(eng_open_take):]
        rem = avail_eng - len(eng_open_take)
        if eng_closed and rem > 1:
            eng_closed_take = eng_closed[: rem - 1]
            eng_closed_rest = eng_closed[len(eng_closed_take):]
        elif eng_closed and rem == 1:
            eng_closed_rest = list(eng_closed)

    cont_sections: list[tuple[str, list[str]]] = []
    if status_rest:
        cont_sections.append(("By Status (continued)", status_rest))
    if prio_rest:
        cont_sections.append(("By Priority (continued)", prio_rest))
    if recent_rest:
        cont_sections.append(
            ("Recent Issues (continued)",
             [f"{r['key']}  {r['status'][:8]:8s}  {r['summary'][:52]}" for r in recent_rest])
        )
    if esc_rest:
        cont_sections.append(
            ("Escalated (continued)",
             [f"{e['key']}  {e['summary'][:48]}  ({e['status']})" for e in esc_rest])
        )
    eng_cont: list[str] = []
    if eng_open_rest:
        for t in eng_open_rest:
            assignee = t.get("assignee") or "unassigned"
            eng_cont.append(f"{t['key']}  {t['summary'][:40]}  [{assignee}]")
    if eng_closed_rest:
        eng_cont.append("— Recently Closed —")
        for t in eng_closed_rest:
            eng_cont.append(f"{t['key']}  {t['summary'][:48]}")
    if eng_cont:
        cont_sections.append(("Engineering Pipeline (continued)", eng_cont))

    if cont_sections:
        flat_lines = sum(len(s[1]) + 1 for s in cont_sections)
        per_pg = max(1, int((max_y - body_top - 8) // line_h))
        total_pages = 1 + (flat_lines + per_pg - 1) // per_pg
        total_pages = min(total_pages, MAX_PAGINATED_SLIDE_PAGES)
    else:
        total_pages = 1

    page_sid0 = f"{sid}_p0" if total_pages > 1 else sid
    oids.append(page_sid0)
    _slide(reqs, page_sid0, idx)
    _bg(reqs, page_sid0, WHITE)
    ttl0 = "Support Summary" if total_pages == 1 else f"Support Summary (1 of {total_pages})"
    _slide_title(reqs, page_sid0, ttl0)
    _box(reqs, f"{page_sid0}_hdr", page_sid0, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{page_sid0}_hdr", 0, len(header), size=11, color=NAVY, font=FONT, bold=True)
    if sla_lines:
        _box(reqs, f"{page_sid0}_sla", page_sid0, MARGIN, BODY_Y + 18, CONTENT_W, 12 * len(sla_lines) + 4, sla_text)
        _style(reqs, f"{page_sid0}_sla", 0, len(sla_text), size=9, color=GRAY, font=FONT)
        fr_label = "First Response:"
        fr_end = sla_text.find(fr_label)
        if fr_end >= 0:
            _style(reqs, f"{page_sid0}_sla", fr_end, fr_end + len(fr_label), bold=True, color=NAVY)
        res_label = "Resolution:"
        res_pos = sla_text.find(res_label)
        if res_pos >= 0:
            _style(reqs, f"{page_sid0}_sla", res_pos, res_pos + len(res_label), bold=True, color=NAVY)

    body_top = BODY_Y + body_offset
    left_y = body_top
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    if st_take:
        st_footer = ""
        if status_rest:
            k_st = len(st_take)
            sub_st = sum(c for _, c in status_items[:k_st])
            rest_st = sum(c for _, c in status_items[k_st:])
            n_st = len(status_items) - k_st
            st_footer = (
                f"\nSubtotal {sub_st} tickets in the statuses above. "
                f"Next slide(s): {rest_st} tickets across {n_st} more status row(s). "
                f"(All statuses total {sum_status} tickets.)"
            )
        elif sum_status != total:
            st_footer = f"\nNote: status rows sum to {sum_status}; header shows {total} issues."
        status_text = "By Status\n" + "\n".join(st_take) + st_footer
        st_h = sec_title_h + line_h * len(st_take) + 4 + st_footer_h
        _box(reqs, f"{page_sid0}_st", page_sid0, left_x, left_y, left_w, st_h, status_text)
        _style(reqs, f"{page_sid0}_st", 0, len(status_text), size=8, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid0}_st", 0, len("By Status"), bold=True, size=9, color=BLUE)
        if st_footer:
            _st_f0 = len("By Status\n" + "\n".join(st_take))
            _style(reqs, f"{page_sid0}_st", _st_f0, len(status_text), size=7, color=GRAY, font=FONT)
        left_y += st_h + 4
    if pr_take:
        pr_footer = ""
        if prio_rest:
            k_pr = len(pr_take)
            sub_pr = sum(c for _, c in prio_items[:k_pr])
            rest_pr = sum(c for _, c in prio_items[k_pr:])
            n_pr = len(prio_items) - k_pr
            pr_footer = (
                f"\nSubtotal {sub_pr} tickets in the priorities above. "
                f"Next slide(s): {rest_pr} tickets across {n_pr} more priority row(s). "
                f"(All priorities total {sum_priority} tickets.)"
            )
        elif sum_priority != total:
            pr_footer = f"\nNote: priority rows sum to {sum_priority}; header shows {total} issues."
        prio_text = "By Priority\n" + "\n".join(pr_take) + pr_footer
        pr_h = sec_title_h + line_h * len(pr_take) + 4 + pr_footer_h
        _box(reqs, f"{page_sid0}_pr", page_sid0, left_x, left_y, left_w, pr_h, prio_text)
        _style(reqs, f"{page_sid0}_pr", 0, len(prio_text), size=8, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid0}_pr", 0, len("By Priority"), bold=True, size=9, color=BLUE)
        if pr_footer:
            _pr_f0 = len("By Priority\n" + "\n".join(pr_take))
            _style(reqs, f"{page_sid0}_pr", _pr_f0, len(prio_text), size=7, color=GRAY, font=FONT)
        left_y += pr_h + 4

    if recent_take:
        recent_lines = [f"{r['key']}  {r['status'][:8]:8s}  {r['summary'][:30]}" for r in recent_take]
        recent_text = "Recent Issues\n" + "\n".join(recent_lines)
        _box(reqs, f"{page_sid0}_rc", page_sid0, left_x, left_y, left_w, max_y - left_y, recent_text)
        _style(reqs, f"{page_sid0}_rc", 0, len(recent_text), size=8, color=NAVY, font=MONO)
        _style(reqs, f"{page_sid0}_rc", 0, len("Recent Issues"), bold=True, size=9, color=BLUE, font=FONT)
        if jira_base:
            offset = len("Recent Issues\n")
            for r in recent_take:
                key = r["key"]
                _style(reqs, f"{page_sid0}_rc", offset, offset + len(key), bold=True, size=8,
                       color=BLUE, font=MONO, link=f"{jira_base}/browse/{key}")
                offset += len(f"{key}  {r['status'][:8]:8s}  {r['summary'][:30]}") + 1

    right_y = body_top
    if show_esc_block_p1:
        esc_lines = [f"{e['key']}  {e['summary'][:36]}  ({e['status']})" for e in esc_take]
        esc_text = f"Escalated ({esc})\n" + "\n".join(esc_lines)
        esc_h = line_h * (len(esc_take) + 1) + 6
        _box(reqs, f"{page_sid0}_esc", page_sid0, right_x, right_y, right_w, esc_h, esc_text)
        _style(reqs, f"{page_sid0}_esc", 0, len(esc_text), size=8, color=NAVY, font=FONT)
        esc_hdr = f"Escalated ({esc})"
        _style(reqs, f"{page_sid0}_esc", 0, len(esc_hdr), bold=True, size=9,
               color={"red": 0.85, "green": 0.15, "blue": 0.15})
        if jira_base and esc_take:
            offset = len(esc_hdr) + 1
            for e in esc_take:
                key = e["key"]
                _style(reqs, f"{page_sid0}_esc", offset, offset + len(key), bold=True, size=8,
                       color={"red": 0.85, "green": 0.15, "blue": 0.15},
                       link=f"{jira_base}/browse/{key}")
                offset += len(f"{key}  {e['summary'][:36]}  ({e['status']})") + 1
        right_y += esc_h + 4

    if eng_open_take or eng_closed_take:
        eng_lines_body = [eng_hdr]
        for t in eng_open_take:
            assignee = t.get("assignee") or "unassigned"
            eng_lines_body.append(f"  {t['key']}  {t['summary'][:26]}  [{assignee}]")
        if eng_closed_take:
            eng_lines_body.append("Recently Closed")
            for t in eng_closed_take:
                eng_lines_body.append(f"  {t['key']}  {t['summary'][:36]}")
        eng_text = "\n".join(eng_lines_body)
        _box(reqs, f"{page_sid0}_eng", page_sid0, right_x, right_y, right_w, max_y - right_y, eng_text)
        _style(reqs, f"{page_sid0}_eng", 0, len(eng_text), size=8, color=NAVY, font=MONO)
        _style(reqs, f"{page_sid0}_eng", 0, len(eng_hdr), bold=True, size=9, color=BLUE, font=FONT)
        rc_start = eng_text.find("Recently Closed")
        if rc_start >= 0:
            _style(reqs, f"{page_sid0}_eng", rc_start, rc_start + len("Recently Closed"),
                   bold=True, size=8, color=GRAY, font=FONT)
        if jira_base:
            off = 0
            for t in eng_open_take:
                key = t["key"]
                p = eng_text.find(key, off)
                if p >= 0:
                    _style(reqs, f"{page_sid0}_eng", p, p + len(key), bold=True, size=8, color=BLUE, font=MONO,
                           link=f"{jira_base}/browse/{key}")
                    off = p + len(key)
            off = rc_start + len("Recently Closed") if rc_start >= 0 else 0
            for t in eng_closed_take:
                key = t["key"]
                p = eng_text.find(key, off)
                if p >= 0:
                    _style(reqs, f"{page_sid0}_eng", p, p + len(key), bold=True, size=8, color=BLUE, font=MONO,
                           link=f"{jira_base}/browse/{key}")
                    off = p + len(key)

    if cont_sections:
        _, extra_oids = _continuation_pages(
            cont_sections, 1, total_pages, body_top,
            max_slide_index_exclusive=MAX_PAGINATED_SLIDE_PAGES,
            reconcile_header_total=total,
        )
        oids.extend(extra_oids)

    if len(oids) == 1:
        return idx + 1
    return idx + len(oids), oids


def _customer_ticket_metrics_slide(reqs, sid, report, idx):
    """Single-customer support ticket dashboard with KPI cards and ranked charts."""
    jira = report.get("jira") or {}
    snap = jira.get("customer_ticket_metrics") or {}
    charts = report.get("_charts")
    if snap.get("error"):
        return _missing_data_slide(reqs, sid, report, idx, f"Customer ticket metrics: {snap.get('error')}")
    if not snap or not charts:
        return _missing_data_slide(reqs, sid, report, idx, "Customer ticket metrics and chart service")

    customer = report.get("customer") or snap.get("customer") or "Customer"
    entry = report.get("_current_slide") or {}
    title = entry.get("title") or f"{customer} Ticket Metrics"

    unresolved = int(snap.get("unresolved_count") or 0)
    resolved_6mo = int(snap.get("resolved_in_6mo_count") or 0)
    ttfr = snap.get("ttfr_1y") or {}
    ttr = snap.get("ttr_1y") or {}
    adherence = snap.get("sla_adherence_1y") or {}
    by_type = snap.get("by_type_open") or {}
    by_status = snap.get("by_status_open") or {}

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    row_gap = 14
    col_gap = 18
    top_card_w = (CONTENT_W - 2 * col_gap) / 3
    bot_card_w = (CONTENT_W - col_gap) / 2
    card_h = 54
    row1_y = BODY_Y + 8
    row2_y = row1_y + card_h + row_gap

    adherence_pct = adherence.get("pct")
    adherence_value = "—" if adherence_pct is None else f"{adherence_pct:.0f}%"

    _kpi_metric_card(
        reqs, f"{sid}_k1", sid, MARGIN, row1_y, top_card_w, card_h,
        "Unresolved tickets", f"{unresolved}", accent=BLUE,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k2", sid, MARGIN + top_card_w + col_gap, row1_y, top_card_w, card_h,
        "Resolved in last 6 months", f"{resolved_6mo}", accent=BLUE,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k3", sid, MARGIN + 2 * (top_card_w + col_gap), row1_y, top_card_w, card_h,
        "SLA adherence (1y)", adherence_value,
        accent=_GREEN if (adherence_pct or 0) >= 90 else (BLUE if (adherence_pct or 0) >= 75 else _RED),
    )

    _kpi_metric_card(
        reqs, f"{sid}_k4", sid, MARGIN, row2_y, bot_card_w, card_h,
        "TTR (1y median)", ttr.get("median", "—"), accent=BLUE,
    )
    _kpi_metric_card(
        reqs, f"{sid}_k5", sid, MARGIN + bot_card_w + col_gap, row2_y, bot_card_w, card_h,
        "TTFR (1y median)", ttfr.get("median", "—"), accent=BLUE,
    )

    chart_gap = 20
    chart_w = (CONTENT_W - chart_gap) / 2
    # Leave room below slide-level headers so they do not overlap the Sheets chart plot area.
    chart_header_h = 20
    chart_title_y = row2_y + card_h + 14
    chart_y = chart_title_y + chart_header_h + 10
    chart_h = max(96, BODY_BOTTOM - 4 - chart_y)
    left_x = MARGIN
    right_x = MARGIN + chart_w + chart_gap

    def _chart_rows(items: dict[str, int], limit: int = 6) -> tuple[list[str], list[int]]:
        pairs = list(items.items())
        if len(pairs) > limit:
            shown = pairs[: limit - 1]
            other = sum(v for _, v in pairs[limit - 1:])
            shown.append(("Other", other))
        else:
            shown = pairs
        labels = []
        values = []
        for name, count in shown:
            compact = name if len(name) <= 26 else f"{name[:23]}..."
            labels.append(compact)
            values.append(count)
        return labels, values

    type_labels, type_values = _chart_rows(by_type)
    status_labels, status_values = _chart_rows(by_status)

    type_hdr = "Unresolved tickets by type"
    status_hdr = "Unresolved tickets by status"
    _box(reqs, f"{sid}_type_h", sid, left_x, chart_title_y, chart_w, chart_header_h, type_hdr)
    _style(reqs, f"{sid}_type_h", 0, len(type_hdr), bold=True, size=10, color=NAVY, font=FONT)
    _box(reqs, f"{sid}_status_h", sid, right_x, chart_title_y, chart_w, chart_header_h, status_hdr)
    _style(reqs, f"{sid}_status_h", 0, len(status_hdr), bold=True, size=10, color=NAVY, font=FONT)

    from .charts import embed_chart

    # +8 pt vs prior 12 for category / value axis text (bar labels were too small at slide scale).
    _ticket_bar_axis_pt = 20

    if type_labels:
        ss_id, chart_id = charts.add_bar_chart(
            title="Unresolved tickets by type",
            labels=type_labels,
            series={"Open tickets": type_values},
            horizontal=True,
            show_title=False,
            axis_font_size=_ticket_bar_axis_pt,
        )
        embed_chart(reqs, f"{sid}_type_chart", sid, ss_id, chart_id, left_x, chart_y, chart_w, chart_h, linked=False)

    if status_labels:
        ss_id2, chart_id2 = charts.add_bar_chart(
            title="Unresolved tickets by status",
            labels=status_labels,
            series={"Open tickets": status_values},
            horizontal=True,
            show_title=False,
            axis_font_size=_ticket_bar_axis_pt,
        )
        embed_chart(reqs, f"{sid}_status_chart", sid, ss_id2, chart_id2, right_x, chart_y, chart_w, chart_h, linked=False)

    return idx + 1


def _customer_help_recent_slide(
    reqs: list,
    sid: str,
    report: dict,
    idx: int,
    *,
    closed: bool,
) -> int | tuple[int, list[str]]:
    """Shared list slide for HELP tickets opened or resolved in a recent window."""
    jira = report.get("jira") or {}
    blob = jira.get("customer_help_recent")
    if not isinstance(blob, dict):
        return _missing_data_slide(
            reqs, sid, report, idx,
            "customer HELP recent tickets (not in report — use support deck data fetch)",
        )
    if blob.get("error"):
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"customer HELP recent tickets: {blob['error']}",
        )

    jira_base = (jira.get("base_url") or "").rstrip("/")
    items: list[dict[str, Any]] = list(
        blob.get("recently_closed" if closed else "recently_opened") or [],
    )
    days = int(blob.get("closed_within_days" if closed else "opened_within_days") or 45)
    customer = blob.get("customer") or report.get("customer") or "Customer"

    entry = report.get("_current_slide") or {}
    base_title = entry.get("title") or (
        "Recently closed HELP tickets" if closed else "Recently opened HELP tickets"
    )
    kind = "Resolved" if closed else "Created"
    total_n = len(items)
    sub = (
        f"project HELP  ·  matched to {customer}  ·  {kind} in the last {days} days  ·  "
        f"{total_n} ticket{'s' if total_n != 1 else ''}"
    )

    body_top = BODY_Y + 24
    max_rows = _list_data_rows_fit_span(
        y_top=body_top,
        y_bottom=BODY_BOTTOM - 10,
        font_body_pt=9,
        reserved_header_lines=0,
        max_rows_cap=28,
    )
    max_rows = max(1, max_rows)

    if not items:
        chunks: list[list[dict[str, Any]]] = [[]]
    else:
        raw_chunks = [items[i : i + max_rows] for i in range(0, len(items), max_rows)]
        chunks = _cap_chunk_list(raw_chunks)
    num_pages = len(chunks)
    oids: list[str] = []

    for p, chunk in enumerate(chunks):
        page_sid = f"{sid}_p{p}" if num_pages > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + p)
        _bg(reqs, page_sid, WHITE)
        page_title = base_title if num_pages == 1 else f"{base_title} ({p + 1} of {num_pages})"
        _slide_title(reqs, page_sid, page_title)
        _box(reqs, f"{page_sid}_sub", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, sub)
        _style(reqs, f"{page_sid}_sub", 0, len(sub), size=9, color=GRAY, font=FONT)

        y = float(body_top)
        line_h = 14.0
        if not chunk:
            empty_msg = f"No HELP tickets in this window ({kind.lower()} in the last {days} days)."
            _box(reqs, f"{page_sid}_empty", page_sid, MARGIN, int(y), CONTENT_W, 40, empty_msg)
            _style(reqs, f"{page_sid}_empty", 0, len(empty_msg), size=10, color=NAVY, font=FONT)
        else:
            for i, it in enumerate(chunk):
                sm = it.get("summary") or ""
                if len(sm) > 46:
                    sm = sm[:43] + "…"
                status = (it.get("status") or "—")[:22]
                when = (it.get("resolved_short") if closed else it.get("created_short")) or "—"
                key = it.get("key") or "?"
                line = f"{key:12}{sm}  ·  {status}  ·  {when}"
                oid = f"{page_sid}_ln{i}"
                _box(reqs, oid, page_sid, MARGIN, int(y), CONTENT_W, int(line_h), line)
                _style(reqs, oid, 0, len(line), size=9, color=NAVY, font=MONO)
                if jira_base and key and key != "?":
                    lk = len(key)
                    _style(
                        reqs, oid, 0, lk, bold=True, size=9, color=BLUE, font=MONO,
                        link=f"{jira_base}/browse/{key}",
                    )
                y += line_h

    if num_pages == 1:
        return idx + 1
    return idx + num_pages, oids


def _support_recent_opened_slide(reqs: list, sid: str, report: dict, idx: int) -> int | tuple[int, list[str]]:
    return _customer_help_recent_slide(reqs, sid, report, idx, closed=False)


def _support_recent_closed_slide(reqs: list, sid: str, report: dict, idx: int) -> int | tuple[int, list[str]]:
    return _customer_help_recent_slide(reqs, sid, report, idx, closed=True)


def _signals_slide(reqs, sid, report, idx):
    signals = report.get("signals", [])
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, "action signals")

    max_signals = max(1, (BODY_BOTTOM - BODY_Y) // 32 - 1)
    chunks = _cap_chunk_list(
        [signals[i : i + max_signals] for i in range(0, len(signals), max_signals)]
    )
    oids: list[str] = []
    for pi, shown in enumerate(chunks):
        page_sid = f"{sid}_p{pi}" if len(chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, LIGHT)
        st = "Notable Signals" if len(chunks) == 1 else f"Notable Signals ({pi + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, st)
        trend_banner = (report.get("signals_trends_display") or "").strip()
        trend_h = 0
        if pi == 0 and trend_banner:
            trend_h = 46
            _box(reqs, f"{page_sid}_trend", page_sid, MARGIN, BODY_Y, CONTENT_W, trend_h - 4, trend_banner)
            _style(
                reqs,
                f"{page_sid}_trend",
                0,
                len(trend_banner),
                size=11,
                color=GRAY,
                font=FONT,
            )
        base = pi * max_signals
        lines = []
        for i, s in enumerate(shown, start=base + 1):
            lines.append(f"{i}.   {s}")
            lines.append("")
        text = "\n".join(lines)
        oid = f"{page_sid}_sig"
        body_top = BODY_Y + trend_h
        body_h = max(120, 290 - trend_h)
        _box(reqs, oid, page_sid, MARGIN, body_top, CONTENT_W, body_h, text)
        _style(reqs, oid, 0, len(text), size=12, color=NAVY, font=FONT)
        off = 0
        for line in lines:
            if line and line[0].isdigit():
                dot = line.index(".")
                _style(reqs, oid, off, off + dot + 1, bold=True, color=BLUE)
            off += len(line) + 1
    return idx + len(chunks), oids


# Substrings in auto-generated Notable Signals lines → hyperlink to the cohort review deck (QBR bundle).
_COHORT_BUNDLE_SIGNAL_LINK_PHRASES: tuple[str, ...] = ("cohort median", "portfolio median")


def _utf16_code_unit_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2 if s else 0


def _slides_shape_text_plain(text_body: dict[str, Any]) -> str:
    parts: list[str] = []
    for te in text_body.get("textElements") or []:
        tr = te.get("textRun")
        if isinstance(tr, dict):
            parts.append(str(tr.get("content") or ""))
    return "".join(parts)


def _utf16_ranges_for_phrases(full: str, phrases: tuple[str, ...]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for phrase in phrases:
        if not phrase:
            continue
        pos = 0
        while True:
            j = full.find(phrase, pos)
            if j < 0:
                break
            u0 = _utf16_code_unit_len(full[:j])
            u1 = u0 + _utf16_code_unit_len(phrase)
            ranges.append((u0, u1))
            pos = j + len(phrase)
    return ranges


def _iter_flat_page_elements(elements: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for el in elements or []:
        grp = el.get("elementGroup")
        if isinstance(grp, dict):
            out.extend(_iter_flat_page_elements(grp.get("children")))
        else:
            out.append(el)
    return out


def apply_cohort_bundle_links_to_notable_signals(
    slides_svc: Any,
    pres_id: str,
    cohort_deck_url: str,
    *,
    page_object_ids: list[str] | None = None,
) -> int:
    """Hyperlink cohort/portfolio median wording on Notable Signals to the cohort review deck.

    Signal bodies are built by ``_signals_slide`` as shapes whose objectId ends with ``_sig``.
    When ``page_object_ids`` is set (QBR exec-summary insert), only those slides are scanned;
    when omitted or empty, every slide is scanned (standalone Executive Summary companion deck).
    """
    link_url = (cohort_deck_url or "").strip()
    if not link_url:
        return 0
    if "/edit" not in link_url:
        link_url = link_url.rstrip("/") + "/edit"

    try:
        pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    except HttpError as e:
        logger.warning("apply_cohort_bundle_links: could not read presentation %s: %s", pres_id[:12], e)
        return 0

    by_id = {s["objectId"]: s for s in pres.get("slides", [])}
    if page_object_ids:
        slides_to_scan = [by_id[pid] for pid in page_object_ids if pid in by_id]
    else:
        slides_to_scan = list(by_id.values())

    reqs: list[dict[str, Any]] = []
    for slide in slides_to_scan:
        for el in _iter_flat_page_elements(slide.get("pageElements")):
            oid = el.get("objectId") or ""
            if not oid.endswith("_sig"):
                continue
            shape = el.get("shape") or {}
            tb = shape.get("text") or {}
            full = _slides_shape_text_plain(tb)
            if not full:
                continue
            for u0, u1 in _utf16_ranges_for_phrases(full, _COHORT_BUNDLE_SIGNAL_LINK_PHRASES):
                if u0 >= u1:
                    continue
                reqs.append({
                    "updateTextStyle": {
                        "objectId": oid,
                        "textRange": {
                            "type": "FIXED_RANGE",
                            "startIndex": u0,
                            "endIndex": u1,
                        },
                        "style": {"link": {"url": link_url}},
                        "fields": "link",
                    }
                })

    if not reqs:
        return 0
    try:
        presentations_batch_update_chunked(slides_svc, pres_id, reqs)
    except HttpError as e:
        logger.warning("apply_cohort_bundle_links: batchUpdate failed for %s: %s", pres_id[:12], e)
        return 0
    logger.info(
        "Linked cohort/portfolio median text → cohort deck (%d span(s)) in presentation %s…",
        len(reqs),
        pres_id[:12],
    )
    return len(reqs)


# ── Portfolio slide builders (cross-customer) ──


def _portfolio_title_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    n = report.get("customer_count", 0)
    days = report.get("days", 30)
    ql = report.get("quarter")
    title = "Book of Business Review"
    sub = f"{n} customers  ·  {_date_range(days, ql, report.get('quarter_start'), report.get('quarter_end'))}"

    _box(reqs, f"{sid}_t", sid, MARGIN, 100, CONTENT_W, 80, title)
    _style(reqs, f"{sid}_t", 0, len(title), bold=True, size=36, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 190, CONTENT_W, 30, sub)
    _style(reqs, f"{sid}_s", 0, len(sub), size=15, color=LTBLUE, font=FONT)

    gen = report.get("generated", "")
    if gen:
        _box(reqs, f"{sid}_d", sid, MARGIN, 340, CONTENT_W, 20, gen)
        _style(reqs, f"{sid}_d", 0, len(gen), size=10, color=GRAY, font=FONT)

    return idx + 1


def _portfolio_signals_slide(reqs, sid, report, idx):
    signals = report.get("portfolio_signals", [])
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio action signals")

    max_rows = 12
    chunks = _cap_chunk_list(
        [signals[i : i + max_rows] for i in range(0, len(signals), max_rows)]
    )
    oids: list[str] = []
    for pi, chunk in enumerate(chunks):
        page_sid = f"{sid}_p{pi}" if len(chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, WHITE)
        st = "Critical Signals Across Portfolio" if len(chunks) == 1 else f"Critical Signals ({pi + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, st)
        y = BODY_Y
        for i, s in enumerate(chunk):
            sev = s.get("severity", 0)
            dot = "\u25cf "
            dot_color = {"red": 0.85, "green": 0.15, "blue": 0.15} if sev >= 2 else \
                        {"red": 0.9, "green": 0.65, "blue": 0.0}
            cust = s["customer"]
            sig = s["signal"]
            line = f"{dot}{cust}:  {sig}"
            _box(reqs, f"{page_sid}_r{i}", page_sid, MARGIN, y, CONTENT_W, 20, line)
            _style(reqs, f"{page_sid}_r{i}", 0, len(line), size=9, color=NAVY, font=FONT)
            _style(reqs, f"{page_sid}_r{i}", 0, len(dot), color=dot_color, size=10)
            _style(reqs, f"{page_sid}_r{i}", len(dot), len(dot) + len(cust), bold=True, size=9)
            y += 22
    return idx + len(chunks), oids


def _portfolio_trends_slide(reqs, sid, report, idx):
    trends_data = report.get("portfolio_trends", {})
    trends = trends_data.get("trends", [])
    if not trends:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio trends")

    type_colors = {
        "concern": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "opportunity": BLUE,
        "positive": {"red": 0.1, "green": 0.6, "blue": 0.2},
        "insight": NAVY,
    }

    per_page = 8
    trend_chunks = _cap_chunk_list(
        [trends[i : i + per_page] for i in range(0, len(trends), per_page)]
    )
    oids: list[str] = []
    for pi, tchunk in enumerate(trend_chunks):
        page_sid = f"{sid}_p{pi}" if len(trend_chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, LIGHT)
        st = "Aggregate Trends" if len(trend_chunks) == 1 else f"Aggregate Trends ({pi + 1} of {len(trend_chunks)})"
        _slide_title(reqs, page_sid, st)
        total_active = trends_data.get("total_active_users", 0)
        total_users = trends_data.get("total_users", 0)
        login_pct = trends_data.get("overall_login_pct", 0)
        header = f"{total_active:,} active users of {total_users:,} total  ·  {login_pct}% login rate"
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 20, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=12, color=NAVY, font=FONT, bold=True)
        y = BODY_Y + 36
        for i, t in enumerate(tchunk):
            trend_type = t.get("type", "insight")
            badge = f"[{trend_type.upper()}]"
            text = t["trend"]
            custs = t.get("customers", "")
            line = f"{badge}  {text}"
            if custs:
                line += f"\n     {custs}"
            _box(reqs, f"{page_sid}_t{i}", page_sid, MARGIN, y, CONTENT_W, 34, line)
            _style(reqs, f"{page_sid}_t{i}", 0, len(line), size=10, color=NAVY, font=FONT)
            _style(reqs, f"{page_sid}_t{i}", 0, len(badge), bold=True, size=10,
                   color=type_colors.get(trend_type, NAVY))
            if custs:
                cust_start = line.index(custs)
                _style(reqs, f"{page_sid}_t{i}", cust_start, cust_start + len(custs),
                       size=8, color=GRAY)
            y += 38
    return idx + len(trend_chunks), oids


def _portfolio_leaders_slide(reqs, sid, report, idx):
    leaders = report.get("portfolio_leaders", {})
    if not leaders:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio leaders")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Customer Leaders")

    categories = [
        ("kei_adoption", "Kei AI Adoption", "adoption_rate", "%"),
        ("executive_engagement", "Executive Engagement", "executives", ""),
        ("write_depth", "Write Depth", "write_ratio", "%"),
        ("export_intensity", "Export Volume", "total_exports", ""),
        ("login_rate", "Weekly Active Rate", "login_pct", "%"),
    ]

    col_w = (CONTENT_W - 20) // 3
    col_h = 150
    positions = [
        (MARGIN, BODY_Y),
        (MARGIN + col_w + 10, BODY_Y),
        (MARGIN + 2 * (col_w + 10), BODY_Y),
        (MARGIN, BODY_Y + col_h + 10),
        (MARGIN + col_w + 10, BODY_Y + col_h + 10),
        (MARGIN + 2 * (col_w + 10), BODY_Y + col_h + 10),
    ]

    for ci, (key, label, metric, unit) in enumerate(categories):
        entries = leaders.get(key, [])
        if not entries or ci >= len(positions):
            continue
        x, y = positions[ci]

        _rect(reqs, f"{sid}_bg{ci}", sid, x, y, col_w, col_h, LIGHT)

        _box(reqs, f"{sid}_cat{ci}", sid, x + 8, y + 6, col_w - 16, 18, label)
        _style(reqs, f"{sid}_cat{ci}", 0, len(label), bold=True, size=10, color=BLUE, font=FONT)

        lines = []
        for e in entries[:5]:
            val = e.get(metric, 0)
            if isinstance(val, float):
                val = round(val)
            lines.append(f"{e['rank']}.  {e['customer']}  —  {val}{unit}")
        text = "\n".join(lines)

        _box(reqs, f"{sid}_ent{ci}", sid, x + 8, y + 28, col_w - 16, col_h - 34, text)
        _style(reqs, f"{sid}_ent{ci}", 0, len(text), size=9, color=NAVY, font=FONT)

        off = 0
        for line in lines:
            dot_end = line.index(".")
            _style(reqs, f"{sid}_ent{ci}", off, off + dot_end + 1, bold=True, color=BLUE, size=9)
            off += len(line) + 1

    return idx + 1


def _cohort_summary_slide(reqs, sid, report, idx):
    """Portfolio-wide cohort summary — aggregate KPIs across all cohorts."""
    m = _cohort_summary_metrics(report)
    if not m:
        return _missing_data_slide(reqs, sid, report, idx, "cohort_digest (no cohort data)")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)

    L = _CohortSummaryLabels
    total_customers = m["total_customers"]
    num_cohorts = m["num_cohorts"]
    total_users = m["total_users"]
    total_active = m["total_active"]
    overall_active_pct = m["overall_active_pct"]
    total_arr = m["total_arr"]
    med_login = m["med_login"]
    med_write = m["med_write"]
    med_exports = m["med_exports"]
    med_kei = m["med_kei"]
    biggest_lbl = m["biggest_lbl"]

    ttl = "Cohort Summary"
    _slide_title(reqs, sid, ttl)

    row1_y = BODY_Y + 8
    card_h = 58
    gap = 12
    cards_per_row = 3
    card_w = (CONTENT_W - gap * (cards_per_row - 1)) / cards_per_row

    _kpi_metric_card(reqs, f"{sid}_c0", sid,
                     MARGIN, row1_y, card_w, card_h,
                     L.TOTAL_CUSTOMERS, str(total_customers), accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c1", sid,
                     MARGIN + card_w + gap, row1_y, card_w, card_h,
                     L.COHORTS, str(num_cohorts), accent=BLUE)
    arr_str = _fmt_platform_value_dollar(total_arr) if total_arr > 0 else "—"
    _kpi_metric_card(reqs, f"{sid}_c2", sid,
                     MARGIN + 2 * (card_w + gap), row1_y, card_w, card_h,
                     L.TOTAL_ARR, arr_str, accent=BLUE)

    row2_y = row1_y + card_h + gap
    _kpi_metric_card(reqs, f"{sid}_c3", sid,
                     MARGIN, row2_y, card_w, card_h,
                     L.TOTAL_USERS, f"{total_users:,}", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c4", sid,
                     MARGIN + card_w + gap, row2_y, card_w, card_h,
                     L.ACTIVE_USERS_7D, f"{total_active:,}", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c5", sid,
                     MARGIN + 2 * (card_w + gap), row2_y, card_w, card_h,
                     L.ACTIVE_RATE, f"{overall_active_pct}%", accent=BLUE)

    row3_y = row2_y + card_h + gap
    _kpi_metric_card(reqs, f"{sid}_c6", sid,
                     MARGIN, row3_y, card_w, card_h,
                     L.WEEKLY_ACTIVE_MEDIAN, f"{med_login}%" if med_login is not None else "—", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c7", sid,
                     MARGIN + card_w + gap, row3_y, card_w, card_h,
                     L.WRITE_RATIO_MEDIAN, f"{med_write}%" if med_write is not None else "—", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c8", sid,
                     MARGIN + 2 * (card_w + gap), row3_y, card_w, card_h,
                     L.KEI_ADOPTION_MEDIAN, f"{med_kei}%" if med_kei is not None else "—", accent=BLUE)

    row4_y = row3_y + card_h + gap
    cards_r4 = 2
    card_w4 = (CONTENT_W - gap * (cards_r4 - 1)) / cards_r4
    _kpi_metric_card(reqs, f"{sid}_c9", sid,
                     MARGIN, row4_y, card_w4, card_h,
                     L.EXPORTS_MEDIAN, f"{med_exports:.0f}" if med_exports is not None else "—", accent=BLUE)
    _kpi_metric_card(reqs, f"{sid}_c10", sid,
                     MARGIN + card_w4 + gap, row4_y, card_w4, card_h,
                     L.LARGEST_COHORT, biggest_lbl, accent=BLUE, value_pt=14)

    return idx + 1


def _cohort_deck_title_slide(reqs, sid, report, idx):
    """Title for manufacturing cohort deck (uses same portfolio report payload)."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    n = report.get("customer_count", 0)
    days = report.get("days", 30)
    ql = report.get("quarter")
    title = "Manufacturing cohort review"
    sub = f"{n} customers in scope  ·  {_date_range(days, ql, report.get('quarter_start'), report.get('quarter_end'))}"

    _box(reqs, f"{sid}_t", sid, MARGIN, 100, CONTENT_W, 80, title)
    _style(reqs, f"{sid}_t", 0, len(title), bold=True, size=32, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 188, CONTENT_W, 36, sub)
    _style(reqs, f"{sid}_s", 0, len(sub), size=14, color=LTBLUE, font=FONT)

    note = "Cohorts from cohorts.yaml · see docs/CUSTOMER_COHORTS.md"
    _box(reqs, f"{sid}_n", sid, MARGIN, 240, CONTENT_W, 20, note)
    _style(reqs, f"{sid}_n", 0, len(note), size=10, color=GRAY, font=FONT)

    gen = report.get("generated", "")
    if gen:
        _box(reqs, f"{sid}_d", sid, MARGIN, 340, CONTENT_W, 20, gen)
        _style(reqs, f"{sid}_d", 0, len(gen), size=10, color=GRAY, font=FONT)

    return idx + 1


def _cohort_profiles_slide(reqs, sid, report, idx) -> int | tuple[int, list[str]]:
    """Up to ``rollup_params.max_physical_slides`` cohort profile pages (see cohort-01-profiles.yaml)."""
    digest = report.get("cohort_digest") or {}
    cap = cohort_profiles_max_physical_slides()
    rows = sorted(
        [(k, v) for k, v in digest.items() if isinstance(v, dict) and int(v.get("n") or 0) > 0],
        key=lambda x: (x[0] == "unclassified", -int(x[1].get("n") or 0)),
    )[:cap]
    if not rows:
        return _missing_data_slide(reqs, sid, report, idx, "cohort_digest (no customers in cohort buckets)")

    total_customers = report.get("customer_count", 0)
    arr_map = report.get("_arr_by_customer") or {}
    oids: list[str] = []
    blocks_for_notes: list[dict[str, Any]] = []
    num = len(rows)
    for pi, (_cid, block) in enumerate(rows):
        page_sid = f"{sid}_p{pi}" if num > 1 else sid
        oids.append(page_sid)
        blocks_for_notes.append(block)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, WHITE)
        cohort_n = block["n"]
        cohort_arr = sum(arr_map.get(c, 0) for c in (block.get("customers") or []))
        ttl = f"{block['display_name']} ({cohort_n} of {total_customers} customers"
        if cohort_arr > 0:
            ttl += f", {_fmt_platform_value_dollar(cohort_arr)} ARR"
        ttl += ")"
        _slide_title(reqs, page_sid, ttl)

        mlogin = block.get("median_login_pct")
        mlogin_s = "—" if mlogin is None else f"{mlogin}%"
        mw = block.get("median_write_ratio")
        mw_s = "—" if mw is None else f"{mw}%"
        me = block.get("median_exports")
        me_s = "—" if me is None else f"{me:.0f}"

        hdr = (
            f"{block['total_active_users']:,} active (7d) / "
            f"{block['total_users']:,} total users across cohort"
        )
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 20, hdr)
        _style(reqs, f"{page_sid}_hdr", 0, len(hdr), size=12, color=GRAY, font=FONT)

        kei_pct = block.get("kei_adoption_pct", 0)
        stats = (
            f"Weekly active rate (median) {mlogin_s}  ·  "
            f"write-to-total ratio (median) {mw_s}  ·  "
            f"Kei adopters (% of customers) {kei_pct}%  ·  "
            f"exports per customer (median, 30d) {me_s}"
        )
        _box(reqs, f"{page_sid}_st", page_sid, MARGIN, BODY_Y + 24, CONTENT_W, 36, stats)
        _style(reqs, f"{page_sid}_st", 0, len(stats), size=12, color=NAVY, font=FONT)

        customers = block.get("customers") or []
        arr_map = report.get("_arr_by_customer") or {}

        def _fmt_arr(v: float) -> str:
            av = abs(v)
            if av >= 1_000_000:
                return f"${v / 1_000_000:,.1f}M"
            if av >= 1_000:
                return f"${v / 1_000:,.0f}K"
            return f"${v:,.0f}"

        decorated = [(n, arr_map.get(n, 0.0)) for n in customers]
        decorated.sort(key=lambda x: -x[1])

        def _label(name: str, arr: float) -> str:
            return f"• {name} — {_fmt_arr(arr)}" if arr else f"• {name}"

        mid = (len(decorated) + 1) // 2
        col_left = decorated[:mid]
        col_right = decorated[mid:]

        acc_y = BODY_Y + 66
        acc_h = BODY_BOTTOM - acc_y - 8
        col_w = (CONTENT_W - 24) // 2

        has_arr = any(arr > 0 for _, arr in decorated)
        left_hdr = "Accounts (by ARR)" if has_arr else "Accounts"
        left_lines = [left_hdr] + [_label(n, a) for n, a in col_left]
        left_body = "\n".join(left_lines)
        _wrap_box(reqs, f"{page_sid}_accL", page_sid, MARGIN, acc_y, col_w, acc_h, left_body)
        _style(reqs, f"{page_sid}_accL", 0, len(left_body), size=11, color=NAVY, font=FONT)
        _style(reqs, f"{page_sid}_accL", 0, len(left_hdr), bold=True, size=12, color=BLUE, font=FONT)

        if col_right:
            right_lines = [""] + [_label(n, a) for n, a in col_right]
            right_body = "\n".join(right_lines)
            _wrap_box(reqs, f"{page_sid}_accR", page_sid, MARGIN + col_w + 24, acc_y, col_w, acc_h, right_body)
            _style(reqs, f"{page_sid}_accR", 0, len(right_body), size=11, color=NAVY, font=FONT)

    report["_cohort_profile_speaker_note_blocks"] = blocks_for_notes

    if num == 1:
        return idx + 1
    return idx + num, oids


# Vertical budget per numbered finding (body band — same idea as portfolio trends / signals rows).
_COHORT_FINDING_ROW_H_PT = 38
_COHORT_FINDING_ROW_GAP_PT = 6


def _cohort_findings_rows_per_page() -> int:
    """How many wrapped bullet rows fit between BODY_Y and BODY_BOTTOM (matches list pagination elsewhere)."""
    avail = float(BODY_BOTTOM) - float(BODY_Y) - 8.0
    step = float(_COHORT_FINDING_ROW_H_PT + _COHORT_FINDING_ROW_GAP_PT)
    return max(1, int(avail // step))


def _cohort_findings_slide(reqs, sid, report, idx):
    bullets = list(report.get("cohort_findings_bullets") or [])
    if not bullets:
        return _missing_data_slide(reqs, sid, report, idx, "cohort_findings_bullets")

    max_rows = _cohort_findings_rows_per_page()
    max_rows = min(max_rows, 28)
    chunks = _cap_chunk_list(
        [bullets[i : i + max_rows] for i in range(0, len(bullets), max_rows)]
    )
    oids: list[str] = []
    for pi, chunk in enumerate(chunks):
        page_sid = f"{sid}_p{pi}" if len(chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, LIGHT)
        st = (
            "Notable findings — cohort differences"
            if len(chunks) == 1
            else f"Notable findings — cohort differences ({pi + 1} of {len(chunks)})"
        )
        _slide_title(reqs, page_sid, st)
        base = pi * max_rows
        y = float(BODY_Y)
        for i, raw in enumerate(chunk, start=base + 1):
            line = raw if len(raw) <= 220 else raw[:217] + "…"
            prefix = f"{i}.   "
            full = f"{prefix}{line}"
            oid_b = f"{page_sid}_cf{i}"
            _wrap_box(
                reqs,
                oid_b,
                page_sid,
                MARGIN,
                int(y),
                CONTENT_W,
                _COHORT_FINDING_ROW_H_PT,
                full,
            )
            _style(reqs, oid_b, 0, len(full), size=12, color=NAVY, font=FONT)
            _style(reqs, oid_b, 0, len(prefix), bold=True, color=BLUE)
            y += float(_COHORT_FINDING_ROW_H_PT + _COHORT_FINDING_ROW_GAP_PT)
    return idx + len(chunks), oids


# ── Data Quality slide ──

_GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}   # #21a659
_AMBER = {"red": 0.9,  "green": 0.65, "blue": 0.0}    # #e6a600
_RED   = {"red": 0.85, "green": 0.15, "blue": 0.15}    # #d92626

_SEV_COLOR = {"ERROR": _RED, "WARNING": _AMBER, "INFO": GRAY}
_SEV_DOT   = {"ERROR": "\u2716", "WARNING": "\u26a0", "INFO": "\u2139"}


def _data_quality_slide(reqs, sid, report, idx):
    from .qa import qa
    snap = qa.summary(report=report)

    max_rows = 10
    flags = snap["flags"]
    sorted_flags = sorted(flags, key=lambda f: {"ERROR": 0, "WARNING": 1, "INFO": 2}.get(f["severity"], 3))
    flag_chunks = _cap_chunk_list(
        [sorted_flags[i : i + max_rows] for i in range(0, len(sorted_flags), max_rows)]
    )
    if not flag_chunks:
        flag_chunks = [[]]
    num_pages = len(flag_chunks)
    oids: list[str] = []

    def _render_flag_row(page_sid: str, fi: int, f: dict, y_pos: float) -> None:
        sev = f["severity"]
        dot = _SEV_DOT.get(sev, "?")
        dot_color = _SEV_COLOR.get(sev, GRAY)
        msg = f["message"]
        detail_parts = []
        if f["expected"] is not None and f["actual"] is not None:
            detail_parts.append(f"expected {f['expected']}, got {f['actual']}")
        if f["sources"]:
            detail_parts.append(" vs ".join(f["sources"]))
        line = f"{dot}  {msg}"
        detail = ""
        if detail_parts:
            detail = f"    {' · '.join(detail_parts)}"
        full = line + detail
        if len(full) > 120:
            full = full[:117] + "..."
        oid = f"{page_sid}_f{fi}"
        _box(reqs, oid, page_sid, MARGIN, y_pos, CONTENT_W, 18, full)
        _style(reqs, oid, 0, len(full), size=9, color=NAVY, font=FONT)
        _style(reqs, oid, 0, len(dot), color=dot_color, size=10, bold=True)
        if detail:
            _style(reqs, oid, len(line), len(full), color=GRAY, size=8)

    for pi, chunk in enumerate(flag_chunks):
        page_sid = f"{sid}_p{pi}" if num_pages > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, LIGHT)
        if pi == 0:
            _slide_title(reqs, page_sid, "Data Quality")
            sources = snap.get("data_sources", {})
            src_x = MARGIN
            src_y = BODY_Y
            for si, (name, status) in enumerate(sources.items()):
                if status == "ok":
                    icon, color = "\u2713", _GREEN
                else:
                    icon, color = "\u2717", _AMBER
                label = f"{icon} {name}"
                _pill(reqs, f"{page_sid}_src{si}", page_sid, src_x, src_y, 120, 22, label, WHITE, color)
                src_x += 130
            total_checks = snap["total_checks"]
            total_flags = snap["total_flags"]
            n_errors = snap["errors"]
            n_warnings = snap["warnings"]
            sum_y = src_y + 36
            if total_flags == 0:
                status = f"All {total_checks} cross-source checks passed"
                status_color = _GREEN
            elif n_errors > 0:
                status = (
                    f"{n_errors} error{'s' if n_errors != 1 else ''} and "
                    f"{n_warnings} warning{'s' if n_warnings != 1 else ''} found"
                )
                status_color = _RED
            else:
                status = f"{n_warnings} finding{'s' if n_warnings != 1 else ''} to note"
                status_color = _AMBER
            _box(reqs, f"{page_sid}_st", page_sid, MARGIN, sum_y, CONTENT_W, 20, status)
            _style(reqs, f"{page_sid}_st", 0, len(status), bold=True, size=12, color=status_color, font=FONT)
            y = sum_y + 28
        else:
            ttl = f"Data Quality — findings ({pi + 1} of {num_pages})"
            _slide_title(reqs, page_sid, ttl)
            y = BODY_Y

        for i, f in enumerate(chunk):
            _render_flag_row(page_sid, pi * 100 + i, f, y)
            y += 20

        if pi == num_pages - 1:
            note_y = max(y + 6, BODY_BOTTOM - 40)
            note = (
                "Single-source metrics (feature adoption, exports, guides, dollar values) "
                "are not independently verified across sources."
            )
            _box(reqs, f"{page_sid}_note", page_sid, MARGIN, note_y, CONTENT_W, 28, note)
            _style(reqs, f"{page_sid}_note", 0, len(note), size=7, color=GRAY, font=FONT, italic=True)

    return idx + num_pages, oids


# ── CS Report slide builders ──

_HEALTH_BADGE = {
    "GREEN": ({"red": 0.10, "green": 0.55, "blue": 0.28}, "\u2705"),
    "YELLOW": ({"red": 0.9, "green": 0.65, "blue": 0.0}, "\u26a0"),
    "RED": ({"red": 0.78, "green": 0.18, "blue": 0.18}, "\u2716"),
}


def _platform_health_slide(reqs, sid, report, idx):
    cs = get_csr_section(report).get("platform_health") or {}
    site_list = cs.get("sites", [])
    if not site_list:
        return _missing_data_slide(reqs, sid, report, idx, "CS Report platform health / site list")

    dist = cs.get("health_distribution", {})
    total_short = cs.get("total_shortages", 0)
    total_crit = cs.get("total_critical_shortages", 0)
    summary_hdr = "  ·  ".join(
        [f"{v} {k}" for k, v in dist.items() if v > 0]
        + [f"{total_short:,} shortages ({total_crit:,} critical)"]
    )

    ROW_H = 28
    max_rows = max(1, (BODY_BOTTOM - BODY_Y - 24) // ROW_H - 1)
    headers_list = ["Factory", "Health", "CTB%", "CTC%", "Comp Avail%", "Shortages", "Critical"]
    col_widths = [170, 60, 55, 55, 75, 65, 60]
    chunks = _cap_chunk_list(
        [site_list[i : i + max_rows] for i in range(0, len(site_list), max_rows)]
    )
    oids: list[str] = []

    for pi, show in enumerate(chunks):
        page_sid = f"{sid}_p{pi}" if len(chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        ttl = "Platform Health" if len(chunks) == 1 else f"Platform Health ({pi + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, summary_hdr)
        _style(reqs, f"{page_sid}_hdr", 0, len(summary_hdr), size=10, color=GRAY, font=FONT)

        num_rows = 1 + len(show)
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(sum(col_widths), num_rows * ROW_H),
                    "transform": _tf(MARGIN, BODY_Y + 24),
                },
                "rows": num_rows, "columns": len(headers_list),
            }
        })

        def _ct(row, col, text):
            if not text:
                return
            reqs.append({"insertText": {"objectId": table_id,
                         "cellLocation": {"rowIndex": row, "columnIndex": col},
                         "text": text, "insertionIndex": 0}})

        def _cs(row, col, text_len, bold=False, color=None, size=8, align=None):
            if text_len > 0:
                s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
                f = ["fontSize", "fontFamily"]
                if bold:
                    s["bold"] = True; f.append("bold")
                if color:
                    s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": s, "fields": ",".join(f),
                    }
                })
            if align:
                reqs.append({
                    "updateParagraphStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "ALL"},
                        "style": {"alignment": align}, "fields": "alignment",
                    }
                })

        def _cbg(row, col, color):
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_id,
                    "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                    "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                    "fields": "tableCellBackgroundFill",
                }
            })

        _clean_table(reqs, table_id, num_rows, len(headers_list))

        for ci, h in enumerate(headers_list):
            _ct(0, ci, h)
            _cs(0, ci, len(h), bold=True, color=NAVY, size=9, align="END" if ci >= 2 else None)
            _cbg(0, ci, WHITE)

        for ri, s in enumerate(show):
            row = ri + 1
            hs = s.get("health_score") or "NONE"
            badge_info = _HEALTH_BADGE.get(hs)
            badge = badge_info[1] + " " + hs if badge_info else hs
            vals = [
                s.get("factory", "?")[:24],
                badge,
                f'{s.get("clear_to_build_pct", 0):.1f}' if "clear_to_build_pct" in s else "-",
                f'{s.get("clear_to_commit_pct", 0):.1f}' if "clear_to_commit_pct" in s else "-",
                f'{s.get("component_availability_pct", 0):.1f}' if "component_availability_pct" in s else "-",
                f'{s.get("shortages", 0):,}' if "shortages" in s else "-",
                f'{s.get("critical_shortages", 0):,}' if "critical_shortages" in s else "-",
            ]
            for ci, v in enumerate(vals):
                _ct(row, ci, v)
                _cs(row, ci, len(v), color=NAVY, size=8, align="END" if ci >= 2 else None)
                _cbg(row, ci, WHITE)

    return idx + len(chunks), oids


def _supply_chain_slide(reqs, sid, report, idx):
    cs = get_csr_section(report).get("supply_chain") or {}
    site_list = cs.get("sites", [])
    if not site_list:
        return _missing_data_slide(reqs, sid, report, idx, "CS Report supply chain / site list")

    totals = cs.get("totals", {})
    oh = totals.get("on_hand", 0)
    oo = totals.get("on_order", 0)
    ex = totals.get("excess_on_hand", 0)

    def _fmtk(v):
        if v is None or v == 0:
            return "-"
        if abs(v) >= 1_000_000:
            return f"${v/1_000_000:.1f}M"
        if abs(v) >= 1_000:
            return f"${v/1_000:.0f}K"
        return f"${v:,.0f}"

    _SC_KPI_H = 58
    _SC_GAP = 18.0
    _SC_TABLE_TOP = BODY_Y + 8 + _SC_KPI_H + 12

    ROW_H = 28
    max_rows = max(1, (BODY_BOTTOM - _SC_TABLE_TOP) // ROW_H - 1)
    headers_list = ["Factory", "On-Hand", "On-Order", "Excess", "DOI", "Late POs"]
    col_widths = [150, 90, 90, 80, 55, 55]
    chunks = _cap_chunk_list(
        [site_list[i : i + max_rows] for i in range(0, len(site_list), max_rows)]
    )
    oids: list[str] = []

    for pi, show in enumerate(chunks):
        page_sid = f"{sid}_p{pi}" if len(chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        ttl = "Supply Chain Overview" if len(chunks) == 1 else f"Supply Chain Overview ({pi + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, ttl)
        scw = (CONTENT_W - 2 * _SC_GAP) / 3
        kry = BODY_Y + 8
        _kpi_metric_card(
            reqs, f"{page_sid}_k0", page_sid, MARGIN, kry, scw, _SC_KPI_H,
            "Inventory on-hand", _fmtk(oh), accent=BLUE, value_pt=20,
        )
        _kpi_metric_card(
            reqs, f"{page_sid}_k1", page_sid, MARGIN + scw + _SC_GAP, kry, scw, _SC_KPI_H,
            "On-order", _fmtk(oo), accent=BLUE, value_pt=20,
        )
        _kpi_metric_card(
            reqs, f"{page_sid}_k2", page_sid, MARGIN + 2 * (scw + _SC_GAP), kry, scw, _SC_KPI_H,
            "Excess on-hand", _fmtk(ex), accent=BLUE, value_pt=20,
        )

        num_rows = 1 + len(show)
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(sum(col_widths), num_rows * ROW_H),
                    "transform": _tf(MARGIN, _SC_TABLE_TOP),
                },
                "rows": num_rows, "columns": len(headers_list),
            }
        })

        def _ct(row, col, text):
            if not text:
                return
            reqs.append({"insertText": {"objectId": table_id,
                         "cellLocation": {"rowIndex": row, "columnIndex": col},
                         "text": text, "insertionIndex": 0}})

        def _cs(row, col, text_len, bold=False, color=None, size=8, align=None):
            if text_len > 0:
                s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
                f = ["fontSize", "fontFamily"]
                if bold:
                    s["bold"] = True; f.append("bold")
                if color:
                    s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": s, "fields": ",".join(f),
                    }
                })
            if align:
                reqs.append({
                    "updateParagraphStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "ALL"},
                        "style": {"alignment": align}, "fields": "alignment",
                    }
                })

        def _cbg(row, col, color):
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_id,
                    "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                    "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                    "fields": "tableCellBackgroundFill",
                }
            })

        _clean_table(reqs, table_id, num_rows, len(headers_list))

        for ci, h in enumerate(headers_list):
            _ct(0, ci, h)
            _cs(0, ci, len(h), bold=True, color=NAVY, size=9, align="END" if ci >= 1 else None)
            _cbg(0, ci, WHITE)

        for ri, s in enumerate(show):
            row = ri + 1
            vals = [
                s.get("factory", "?")[:22],
                _fmtk(s.get("on_hand_value")),
                _fmtk(s.get("on_order_value")),
                _fmtk(s.get("excess_on_hand")),
                f'{s["doi_days"]:.0f}d' if "doi_days" in s else "-",
                f'{s.get("late_pos", 0):,}' if "late_pos" in s else "-",
            ]
            for ci, v in enumerate(vals):
                _ct(row, ci, v)
                _cs(row, ci, len(v), color=NAVY, size=8, align="END" if ci >= 1 else None)
                _cbg(row, ci, WHITE)

    return idx + len(chunks), oids


def _platform_value_slide(reqs, sid, report, idx):
    cs = get_csr_section(report).get("platform_value") or {}
    total_savings = cs.get("total_savings", 0)
    total_open = cs.get("total_open_ia_value", 0)
    total_recs = cs.get("total_recs_created_30d", 0)
    site_list = cs.get("sites", [])

    total_pos = cs.get("total_pos_placed_30d", 0)
    total_overdue = cs.get("total_overdue_tasks", 0)
    ops = f"{total_pos:,} POs placed  ·  {total_overdue:,} overdue tasks"

    _PV_CARD_H = 58
    _PV_GAP = 18.0

    def _render_kpi(page_sid: str) -> None:
        row_y = BODY_Y + 8
        cw = (CONTENT_W - 2 * _PV_GAP) / 3
        _kpi_metric_card(
            reqs, f"{page_sid}_k0", page_sid, MARGIN, row_y, cw, _PV_CARD_H,
            "Savings achieved", _fmt_platform_value_dollar(total_savings), accent=BLUE, value_pt=22,
        )
        _kpi_metric_card(
            reqs, f"{page_sid}_k1", page_sid, MARGIN + cw + _PV_GAP, row_y, cw, _PV_CARD_H,
            "Open IA pipeline", _fmt_platform_value_dollar(total_open), accent=BLUE, value_pt=22,
        )
        _kpi_metric_card(
            reqs, f"{page_sid}_k2", page_sid, MARGIN + 2 * (cw + _PV_GAP), row_y, cw, _PV_CARD_H,
            "Recs created (30d)", _fmt_platform_value_count(total_recs), accent=BLUE, value_pt=22,
        )
        ops_y = row_y + _PV_CARD_H + 10
        _box(reqs, f"{page_sid}_ops", page_sid, MARGIN, ops_y, CONTENT_W, 16, ops)
        _style(reqs, f"{page_sid}_ops", 0, len(ops), size=9, color=GRAY, font=FONT)

    factory_rows = [s for s in site_list if s.get("savings_current_period") or s.get("recs_created_30d")]
    ROW_H = 28
    tbl_y_kpi = BODY_Y + 8 + _PV_CARD_H + 10 + 16 + 12
    max_rows_first = max(1, (BODY_BOTTOM - tbl_y_kpi) // ROW_H - 1)
    tbl_y_cont = BODY_Y + 24
    max_rows_cont = max(1, (BODY_BOTTOM - tbl_y_cont) // ROW_H - 1)

    chunks_planned: list[list[Any]] = []
    if factory_rows:
        r = list(factory_rows)
        chunks_planned.append(r[:max_rows_first])
        r = r[max_rows_first:]
        while r:
            chunks_planned.append(r[:max_rows_cont])
            r = r[max_rows_cont:]
    chunks_planned = _cap_chunk_list(chunks_planned)

    oids: list[str] = []
    if not chunks_planned:
        _slide(reqs, sid, idx)
        _slide_title(reqs, sid, "Platform Value & ROI")
        _render_kpi(sid)
        return idx + 1, [sid]

    for pi, show in enumerate(chunks_planned):
        page_sid = f"{sid}_p{pi}" if len(chunks_planned) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        if pi == 0:
            _slide_title(reqs, page_sid, "Platform Value & ROI")
            _render_kpi(page_sid)
            tbl_y = tbl_y_kpi
        else:
            _slide_title(
                reqs, page_sid,
                f"Platform Value & ROI — factory detail ({pi + 1} of {len(chunks_planned)})",
            )
            tbl_y = tbl_y_cont

        headers_list = ["Factory", "Savings", "Recs (30d)"]
        col_widths = [180, 120, 80]
        num_rows = 1 + len(show)
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(sum(col_widths), num_rows * ROW_H),
                    "transform": _tf(MARGIN, tbl_y),
                },
                "rows": num_rows, "columns": len(headers_list),
            }
        })

        def _ct(row, col, text):
            if not text:
                return
            reqs.append({"insertText": {"objectId": table_id,
                         "cellLocation": {"rowIndex": row, "columnIndex": col},
                         "text": text, "insertionIndex": 0}})

        def _cs(row, col, text_len, bold=False, color=None, size=8, align=None):
            if text_len > 0:
                s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
                f = ["fontSize", "fontFamily"]
                if bold:
                    s["bold"] = True; f.append("bold")
                if color:
                    s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": s, "fields": ",".join(f),
                    }
                })
            if align:
                reqs.append({
                    "updateParagraphStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "ALL"},
                        "style": {"alignment": align}, "fields": "alignment",
                    }
                })

        def _cbg(row, col, color):
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_id,
                    "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                    "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                    "fields": "tableCellBackgroundFill",
                }
            })

        _clean_table(reqs, table_id, num_rows, len(headers_list))
        for ci, h in enumerate(headers_list):
            _ct(0, ci, h)
            _cs(0, ci, len(h), bold=True, color=NAVY, size=9, align="END" if ci >= 1 else None)
            _cbg(0, ci, WHITE)
        for ri, s in enumerate(show):
            row = ri + 1
            sav_v = s.get("savings_current_period", 0)
            recs_v = s.get("recs_created_30d", 0)
            vals = [
                s.get("factory", "?")[:24],
                f"${sav_v:,.0f}" if sav_v else "-",
                f"{recs_v:,}" if recs_v else "-",
            ]
            for ci, v in enumerate(vals):
                _ct(row, ci, v)
                _cs(row, ci, len(v), color=NAVY, size=8, align="END" if ci >= 1 else None)
                _cbg(row, ci, WHITE)

    return idx + len(chunks_planned), oids


# ── Team roster slide ──

def _load_teams() -> dict[str, Any]:
    """Load team rosters from teams.yaml (project root)."""
    import yaml
    path = Path(__file__).resolve().parent.parent / "teams.yaml"
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}


def _team_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)

    customer = report.get("customer", "Customer")
    teams = _load_teams()
    team_data = teams.get(customer, {})
    cust_members = [m.get("name", "") for m in team_data.get("customer_team", [])]
    ldna_members = [m.get("name", "") for m in team_data.get("leandna_team", [])]

    if not cust_members and not ldna_members:
        cust_members = ["(no team roster configured)"]
        ldna_members = ["(no team roster configured)"]

    # Right panel: blue branded area
    panel_x = 310
    panel_w = SLIDE_W - panel_x
    _rect(reqs, f"{sid}_rpanel", sid, panel_x, 0, panel_w, SLIDE_H, BLUE)

    # Gradient overlay: darker navy strip at right edge
    _rect(reqs, f"{sid}_rnav", sid, SLIDE_W - 80, 0, 80, SLIDE_H, NAVY)

    # "LeanDNA.com" text on the blue panel
    brand = "LeanDNA.com"
    _box(reqs, f"{sid}_brand", sid, panel_x + 40, SLIDE_H - 60, 200, 30, brand)
    _style(reqs, f"{sid}_brand", 0, len(brand), bold=True, size=16, color=WHITE, font=FONT)

    # Left panel: white background (default), team rosters
    left_w = panel_x - MARGIN
    y = 30

    # Customer team header
    cust_hdr = f"{customer} Team"
    _box(reqs, f"{sid}_ch", sid, MARGIN, y, left_w, 24, cust_hdr)
    _style(reqs, f"{sid}_ch", 0, len(cust_hdr), bold=True, size=14, color=BLUE, font=FONT)
    y += 30

    # Customer team members
    for i, name in enumerate(cust_members[:12]):
        _box(reqs, f"{sid}_cm{i}", sid, MARGIN, y, left_w, 16, name)
        _style(reqs, f"{sid}_cm{i}", 0, len(name), bold=True, size=10, color=NAVY, font=FONT)
        y += 18

    y += 14

    # LeanDNA team header
    ldna_hdr = "LeanDNA Team"
    _box(reqs, f"{sid}_lh", sid, MARGIN, y, left_w, 24, ldna_hdr)
    _style(reqs, f"{sid}_lh", 0, len(ldna_hdr), bold=True, size=14, color=BLUE, font=FONT)
    y += 30

    # LeanDNA team members
    for i, name in enumerate(ldna_members[:12]):
        _box(reqs, f"{sid}_lm{i}", sid, MARGIN, y, left_w, 16, name)
        _style(reqs, f"{sid}_lm{i}", 0, len(name), bold=True, size=10, color=NAVY, font=FONT)
        y += 18

    return idx + 1


# ── New slides: SLA Health, Cross-Validation, Engineering Pipeline, Enhancement Requests ──


def _sla_health_slide(reqs, sid, report, idx):
    """SLA performance, sentiment distribution, and request type mix. Always appears; shows red banner when no data."""
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(
            reqs, sid, report, idx,
            "Jira support tickets and SLA metrics (no tickets in period or Jira unavailable)",
        )

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Support Health & SLA")

    days = jira.get("days", 90)
    total = jira["total_issues"]
    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    date_range = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"

    header = f"{total} tickets  ·  {date_range}"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 20, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=12, color=GRAY, font=FONT)

    col_gap = 24
    left_x = MARGIN
    left_w = (CONTENT_W - col_gap) // 2
    right_x = MARGIN + left_w + col_gap
    right_w = CONTENT_W - left_w - col_gap
    body_top = BODY_Y + 26
    max_y = BODY_BOTTOM

    # ── LEFT: SLA gauges ──
    y = body_top

    sla_goal = {"ttfr": "48h", "ttr": "160h"}
    sla_label = {"ttfr": "First Response", "ttr": "Resolution"}
    for sla_key in ("ttfr", "ttr"):
        sla = jira.get(sla_key, {})
        measured = sla.get("measured", 0)
        if measured == 0:
            continue
        label = sla_label[sla_key]
        goal = sla_goal[sla_key]
        breached = sla.get("breached", 0)
        breach_pct = round(100 * breached / max(measured, 1))

        if breach_pct == 0:
            badge_color = {"red": 0.13, "green": 0.55, "blue": 0.13}
            badge_label = "On track"
        elif breach_pct <= 20:
            badge_color = {"red": 0.85, "green": 0.65, "blue": 0.0}
            badge_label = "Caution"
        else:
            badge_color = {"red": 0.85, "green": 0.15, "blue": 0.15}
            badge_label = "At risk"

        title_text = f"{label}  (goal: {goal})"
        _box(reqs, f"{sid}_{sla_key}_t", sid, left_x, y, left_w, 20, title_text)
        _style(reqs, f"{sid}_{sla_key}_t", 0, len(label), bold=True, size=13, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_{sla_key}_t", len(label), len(title_text), size=10, color=GRAY, font=FONT)
        y += 24

        _pill(reqs, f"{sid}_{sla_key}_b", sid, left_x, y, 88, 22, badge_label, badge_color, WHITE)

        breach_txt = "0 breaches" if breached == 0 else f"{breached} breach{'es' if breached != 1 else ''}"
        stats = f"Median {sla.get('median', '—')}  ·  Avg {sla.get('avg', '—')}  ·  {breach_txt} (of {measured} closed)"
        _box(reqs, f"{sid}_{sla_key}_s", sid, left_x + 96, y, left_w - 96, 22, stats)
        _style(reqs, f"{sid}_{sla_key}_s", 0, len(stats), size=11, color=NAVY, font=FONT)
        y += 26

        if sla.get("min") and sla.get("max"):
            range_text = f"Range {sla['min']} – {sla['max']}"
            if sla.get("waiting"):
                range_text += f"  ·  {sla['waiting']} open"
            _box(reqs, f"{sid}_{sla_key}_r", sid, left_x, y, left_w, 18, range_text)
            _style(reqs, f"{sid}_{sla_key}_r", 0, len(range_text), size=10, color=GRAY, font=FONT)
            y += 22

        y += 14

    # ── RIGHT: Sentiment + Request Type ──
    right_y = body_top

    sentiment = jira.get("by_sentiment", {})
    sentiment_clean = {k: v for k, v in sentiment.items() if k != "Unknown"}
    if sentiment_clean:
        sent_title = "Ticket sentiment"
        _box(reqs, f"{sid}_sent_t", sid, right_x, right_y, right_w, 20, sent_title)
        _style(reqs, f"{sid}_sent_t", 0, len(sent_title), bold=True, size=13, color=NAVY, font=FONT)
        right_y += 24

        color_map = {
            "Positive": {"red": 0.13, "green": 0.55, "blue": 0.13},
            "Neutral": {"red": 0.5, "green": 0.5, "blue": 0.5},
            "Negative": {"red": 0.85, "green": 0.15, "blue": 0.15},
        }
        sent_total = sum(sentiment_clean.values())
        for si, (name, count) in enumerate(sentiment_clean.items()):
            pct = round(100 * count / max(sent_total, 1))
            bar_w = max(int(pct * (right_w - 120) / 100), 6)
            fill = color_map.get(name, GRAY)
            _bar_rect(reqs, f"{sid}_sb{si}", sid, right_x, right_y, bar_w, 16, fill, outline=GRAY)
            label = f"{name}  {count} ({pct}%)"
            _box(reqs, f"{sid}_sl{si}", sid, right_x + bar_w + 8, right_y, right_w - bar_w - 8, 16, label)
            _style(reqs, f"{sid}_sl{si}", 0, len(label), size=11, color=NAVY, font=FONT)
            right_y += 22
        right_y += 14

    req_types = jira.get("by_request_type", {})
    if req_types:
        rt_title = "Request channels"
        _box(reqs, f"{sid}_rt_t", sid, right_x, right_y, right_w, 20, rt_title)
        _style(reqs, f"{sid}_rt_t", 0, len(rt_title), bold=True, size=13, color=NAVY, font=FONT)
        right_y += 24

        rt_lines = []
        for name, count in list(req_types.items())[:6]:
            rt_lines.append(f"{count:>3}  {name}")
        rt_text = "\n".join(rt_lines)
        rt_h = min(14 * len(rt_lines) + 6, max_y - right_y)
        _box(reqs, f"{sid}_rtl", sid, right_x, right_y, right_w, rt_h, rt_text)
        _style(reqs, f"{sid}_rtl", 0, len(rt_text), size=11, color=NAVY, font=FONT)

    return idx + 1


def _cross_validation_slide(reqs, sid, report, idx):
    """Pendo vs CS Report engagement comparison per site."""
    cs_ph = get_csr_section(report).get("platform_health") or {}
    pendo_sites = report.get("sites", [])

    cs_factories = cs_ph.get("factories", [])
    if not cs_factories and not pendo_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo sites and/or CS Report factories for comparison")

    engagement = report.get("engagement", {})
    pendo_rate = engagement.get("active_rate_7d")
    if pendo_rate is not None:
        pendo_rate = round(pendo_rate)

    header_parts = []
    if pendo_rate is not None:
        header_parts.append(f"Pendo 7-day active rate: {pendo_rate}%")
    cs_buyer_rates = [f["weekly_active_buyers_pct"] for f in cs_factories
                      if f.get("weekly_active_buyers_pct") is not None]
    if cs_buyer_rates:
        cs_avg = round(sum(cs_buyer_rates) / len(cs_buyer_rates))
        header_parts.append(f"CS Report avg active buyers: {cs_avg}%")
    if pendo_rate is not None and cs_buyer_rates:
        diff = abs(pendo_rate - cs_avg)
        if diff <= 15:
            header_parts.append("✓ Consistent")
        else:
            header_parts.append(f"⚠ {diff}pp gap")

    header = "  ·  ".join(header_parts) if header_parts else "Comparing Pendo usage with CS Report metrics"

    ROW_H = 20
    tbl_y = BODY_Y + 24
    max_rows = max(1, (BODY_BOTTOM - tbl_y) // ROW_H - 1)

    pendo_by_site: dict[str, dict] = {}
    for s in pendo_sites:
        name = s.get("sitename") or s.get("site_name", "")
        if name:
            pendo_by_site[name.lower()] = s

    rows: list[tuple[str, str, str, str, str]] = []
    for f in cs_factories:
        fname = f.get("factory_name", "")
        wab = f.get("weekly_active_buyers_pct")
        health = f.get("health_score")
        pendo_match = None
        for pname, ps in pendo_by_site.items():
            if fname.lower() in pname or pname in fname.lower():
                pendo_match = ps
                break

        p_users = str(pendo_match.get("total_visitors", "—")) if pendo_match else "—"
        p_events = _fmt_count(pendo_match.get("total_events", 0)) if pendo_match else "—"
        cs_wab = f"{wab:.0f}%" if wab is not None else "—"
        cs_health_str = f"{health:.0f}" if health is not None else "—"
        rows.append((fname[:22], p_users, p_events, cs_wab, cs_health_str))

    if not rows:
        _slide(reqs, sid, idx)
        _bg(reqs, sid, LIGHT)
        _slide_title(reqs, sid, "Data Cross-Validation")
        note = "No overlapping site data between Pendo and CS Report"
        _box(reqs, f"{sid}_none", sid, MARGIN, tbl_y, CONTENT_W, 30, note)
        _style(reqs, f"{sid}_none", 0, len(note), size=11, color=GRAY, font=FONT)
        return idx + 1

    row_chunks = _cap_chunk_list(
        [rows[i : i + max_rows] for i in range(0, len(rows), max_rows)]
    )
    cols = ["Site", "Pendo Users", "Pendo Events", "CS Active %", "CS Health"]
    col_widths = [150, 90, 100, 90, 90]
    num_cols = len(cols)
    oids: list[str] = []

    for pi, shown in enumerate(row_chunks):
        page_sid = f"{sid}_p{pi}" if len(row_chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, LIGHT)
        ttl = "Data Cross-Validation" if len(row_chunks) == 1 else f"Data Cross-Validation ({pi + 1} of {len(row_chunks)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)

        num_rows = len(shown) + 1
        table_id = f"{page_sid}_tbl"
        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": {"width": {"magnitude": sum(col_widths), "unit": "PT"},
                             "height": {"magnitude": ROW_H * num_rows, "unit": "PT"}},
                    "transform": _tf(MARGIN, tbl_y),
                },
                "rows": num_rows, "columns": num_cols,
            }
        })
        _clean_table(reqs, table_id, num_rows, num_cols)

        for ci, hdr in enumerate(cols):
            reqs.append({"insertText": {"objectId": table_id, "text": hdr,
                                        "cellLocation": {"tableId": table_id, "rowIndex": 0, "columnIndex": ci}}})
        for ri, row in enumerate(shown, 1):
            for ci, val in enumerate(row):
                reqs.append({"insertText": {"objectId": table_id, "text": val,
                                            "cellLocation": {"tableId": table_id, "rowIndex": ri, "columnIndex": ci}}})

    return idx + len(row_chunks), oids


def _engineering_slide(reqs, sid, report, idx):
    """Dedicated slide for engineering work affecting this customer, with GPT-written ticket narratives."""
    jira = report.get("jira", {})
    eng = jira.get("engineering", {})
    eng_open = eng.get("open", [])
    eng_closed = eng.get("recent_closed", [])
    jira_base = jira.get("base_url", "")

    if not eng_open and not eng_closed:
        return _missing_data_slide(reqs, sid, report, idx, "Jira engineering pipeline (in progress / shipped)")

    open_count = eng.get("open_count", len(eng_open))
    closed_count = eng.get("closed_count", len(eng_closed))
    header = f"{eng.get('total', open_count + closed_count)} engineering tickets  ·  {open_count} open  ·  {closed_count} closed"

    TICKET_H = 58
    y0 = BODY_Y + 24
    max_y = BODY_BOTTOM
    per_page = max(1, (max_y - y0 - 24) // TICKET_H)
    seq: list[tuple[str, dict]] = [("o", t) for t in eng_open] + [("c", t) for t in eng_closed]
    pages: list[list[tuple[str, dict]]] = []
    cur: list[tuple[str, dict]] = []
    for item in seq:
        cur.append(item)
        if len(cur) >= per_page:
            pages.append(cur)
            cur = []
    if cur:
        pages.append(cur)
    pages = _cap_chunk_list(pages)

    GREEN = {"red": 0.13, "green": 0.55, "blue": 0.13}
    oids: list[str] = []

    def _render_ticket(page_sid: str, ticket: dict, label_color: dict, prefix: str, counter: int, y_ref: list[float]) -> None:
        y = y_ref[0]
        key = ticket["key"]
        status = (ticket.get("status") or "")[:14]
        assignee = ticket.get("assignee") or "unassigned"
        updated = ticket.get("updated", "")
        summary = ticket.get("summary", "")[:52]
        key_line = f"{key}  {status:14s}  {summary}  [{assignee}]"
        if updated:
            key_line += f"  ({updated})"
        _box(reqs, f"{page_sid}_{prefix}{counter}_k", page_sid, MARGIN, y, CONTENT_W, 14, key_line)
        _style(reqs, f"{page_sid}_{prefix}{counter}_k", 0, len(key_line), size=9, color=NAVY, font=FONT)
        ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
        _style(reqs, f"{page_sid}_{prefix}{counter}_k", 0, len(key), bold=True, size=9,
               color=label_color, font=MONO, link=ticket_url)
        y += 15
        narrative = (ticket.get("narrative") or "").strip()
        if narrative and y + 36 <= max_y:
            _box(reqs, f"{page_sid}_{prefix}{counter}_n", page_sid, MARGIN + 8, y, CONTENT_W - 8, 36, narrative)
            _style(reqs, f"{page_sid}_{prefix}{counter}_n", 0, len(narrative), size=8, color=GRAY, font=FONT)
            y += 38
        y += 6
        y_ref[0] = y

    for pi, page_items in enumerate(pages):
        page_sid = f"{sid}_p{pi}" if len(pages) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, WHITE)
        ttl = "Engineering Pipeline" if len(pages) == 1 else f"Engineering Pipeline ({pi + 1} of {len(pages)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)
        y_ref = [y0]
        last_kind: str | None = None
        for j, (kind, t) in enumerate(page_items):
            if kind == "o" and last_kind != "o":
                open_title = f"In Progress ({open_count})"
                _box(reqs, f"{page_sid}_ot{j}", page_sid, MARGIN, y_ref[0], CONTENT_W, 16, open_title)
                _style(reqs, f"{page_sid}_ot{j}", 0, len(open_title), bold=True, size=11, color=BLUE, font=FONT)
                y_ref[0] += 20
                last_kind = "o"
            elif kind == "c" and last_kind != "c":
                closed_title = f"Recently Shipped ({closed_count})"
                _box(reqs, f"{page_sid}_ct{j}", page_sid, MARGIN, y_ref[0], CONTENT_W, 16, closed_title)
                _style(reqs, f"{page_sid}_ct{j}", 0, len(closed_title), bold=True, size=11, color=GREEN, font=FONT)
                y_ref[0] += 20
                last_kind = "c"
            col = BLUE if kind == "o" else GREEN
            pref = "o" if kind == "o" else "c"
            _render_ticket(page_sid, t, col, pref, pi * 200 + j, y_ref)

    return idx + len(pages), oids


def _enhancement_requests_slide(reqs, sid, report, idx):
    """Customer enhancement requests from the ER project."""
    jira = report.get("jira", {})
    er = jira.get("enhancements", {})
    er_open = er.get("open", [])
    er_shipped = er.get("shipped", [])
    er_declined = er.get("declined", [])
    jira_base = jira.get("base_url", "")

    if not er_open and not er_shipped and not er_declined:
        return _missing_data_slide(reqs, sid, report, idx, "Jira enhancement requests (open / shipped / declined)")

    open_n = er.get("open_count", len(er_open))
    shipped_n = er.get("shipped_count", len(er_shipped))
    declined_n = er.get("declined_count", len(er_declined))
    total = er.get("total", open_n + shipped_n + declined_n)
    header = f"{total} enhancement requests  ·  {open_n} open  ·  {shipped_n} shipped  ·  {declined_n} declined"

    body_top = BODY_Y + 24
    max_y = BODY_BOTTOM
    budget = max_y - body_top
    SEC_TITLE = 20
    ROW_OS = 28
    ROW_DEC = 16
    ER_GREEN = {"red": 0.13, "green": 0.55, "blue": 0.13}

    seq: list[tuple[str, dict]] = (
        [("o", t) for t in er_open] + [("s", t) for t in er_shipped] + [("d", t) for t in er_declined]
    )

    def _row_h(kind: str) -> int:
        return ROW_OS if kind in ("o", "s") else ROW_DEC

    pages: list[list[tuple[str, dict]]] = []
    page: list[tuple[str, dict]] = []
    used = 0
    last_section: str | None = None

    for kind, t in seq:
        row_h = _row_h(kind)
        extra = SEC_TITLE if last_section != kind else 0
        if page and used + extra + row_h > budget:
            pages.append(page)
            page = []
            used = 0
            last_section = None
        if last_section != kind:
            used += SEC_TITLE
            last_section = kind
        page.append((kind, t))
        used += row_h
    if page:
        pages.append(page)
    pages = _cap_chunk_list(pages)

    oids: list[str] = []
    for pi, page_items in enumerate(pages):
        page_sid = f"{sid}_p{pi}" if len(pages) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, WHITE)
        ttl = "Enhancement Requests" if len(pages) == 1 else f"Enhancement Requests ({pi + 1} of {len(pages)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_hdr", page_sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
        _style(reqs, f"{page_sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)

        y = body_top
        last_kind: str | None = None
        for j, (kind, ticket) in enumerate(page_items):
            if last_kind != kind:
                if kind == "o":
                    sec = f"Open ({open_n})"
                    _box(reqs, f"{page_sid}_ot{j}", page_sid, MARGIN, y, CONTENT_W, 16, sec)
                    _style(reqs, f"{page_sid}_ot{j}", 0, len(sec), bold=True, size=10, color=BLUE, font=FONT)
                elif kind == "s":
                    sec = f"Shipped ({shipped_n})"
                    _box(reqs, f"{page_sid}_st{j}", page_sid, MARGIN, y, CONTENT_W, 16, sec)
                    _style(reqs, f"{page_sid}_st{j}", 0, len(sec), bold=True, size=10, color=ER_GREEN, font=FONT)
                else:
                    sec = f"Declined / Deferred ({declined_n})"
                    _box(reqs, f"{page_sid}_dt{j}", page_sid, MARGIN, y, CONTENT_W, 16, sec)
                    _style(reqs, f"{page_sid}_dt{j}", 0, len(sec), bold=True, size=10, color=GRAY, font=FONT)
                y += SEC_TITLE
                last_kind = kind

            key = ticket["key"]
            ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
            if kind == "o":
                prio = ticket.get("priority", "")
                prio_short = prio.split(":")[0] if ":" in prio else prio[:8]
                line1 = f"{key}  {prio_short}"
                line2 = (ticket.get("summary") or "")[:72]
                text = f"{line1}\n{line2}"
                oid = f"{page_sid}_eo{pi}_{j}"
                _box(reqs, oid, page_sid, MARGIN, y, CONTENT_W, 26, text)
                _style(reqs, oid, 0, len(text), size=8, color=NAVY, font=FONT)
                _style(reqs, oid, 0, len(key), bold=True, size=8, color=BLUE, font=MONO, link=ticket_url)
                y += ROW_OS
            elif kind == "s":
                line1 = f"{key}  ({ticket.get('updated', '')})"
                line2 = (ticket.get("summary") or "")[:72]
                text = f"{line1}\n{line2}"
                oid = f"{page_sid}_es{pi}_{j}"
                _box(reqs, oid, page_sid, MARGIN, y, CONTENT_W, 26, text)
                _style(reqs, oid, 0, len(text), size=8, color=NAVY, font=FONT)
                _style(reqs, oid, 0, len(key), bold=True, size=8, color=ER_GREEN, font=MONO, link=ticket_url)
                y += ROW_OS
            else:
                line = f"{key}  {(ticket.get('summary') or '')[:80]}"
                oid = f"{page_sid}_ed{pi}_{j}"
                _box(reqs, oid, page_sid, MARGIN, y, CONTENT_W, 14, line)
                _style(reqs, oid, 0, len(line), size=8, color=GRAY, font=MONO)
                if ticket_url:
                    _style(reqs, oid, 0, len(key), size=8, color=GRAY, font=MONO, link=ticket_url)
                y += ROW_DEC

    return idx + len(pages), oids


# ── QBR template slide builders (LeanDNA APEX styling) ──

# Colors extracted from the Safran QBR template
_BESPOKE_NAVY = {"red": 0.031, "green": 0.239, "blue": 0.471}   # #083d78 accent navy
_BESPOKE_DARK = {"red": 0.031, "green": 0.110, "blue": 0.200}   # #081c33 deep bg

def _qbr_cover_slide(reqs, sid, report, idx):
    """Branded cover slide: customer name, deck title, date."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    customer = report.get("customer", report.get("account", {}).get("customer", ""))
    days = report.get("days", 30)
    quarter_label = report.get("quarter")
    date_str = _date_range(days, quarter_label,
                           report.get("quarter_start"), report.get("quarter_end"))
    raw_date = report.get("generated", "")
    try:
        generated = datetime.datetime.strptime(raw_date, "%Y-%m-%d").strftime("%B %-d, %Y")
    except (ValueError, TypeError):
        generated = raw_date or datetime.datetime.now().strftime("%B %-d, %Y")

    # Decorative tagline (faint, right side)
    tagline = "THE RIGHT PART.\nIN THE RIGHT PLACE.\nAT THE RIGHT TIME."
    _box(reqs, f"{sid}_tag", sid, SLIDE_W - 240, 30, 220, 120, tagline)
    _style(reqs, f"{sid}_tag", 0, len(tagline), size=11, color=_BESPOKE_NAVY, font=FONT,
           bold=True)

    # Main title — generous height so wrapping doesn't overlap the customer name
    title = "Executive business review"
    title_top = SLIDE_H * 0.22
    _box(reqs, f"{sid}_t", sid, MARGIN + 6, title_top, 560, 130, title)
    _style(reqs, f"{sid}_t", 0, len(title), size=50, color=WHITE, font=FONT_SERIF)

    # Customer name — well below the title block
    cust_top = title_top + 140
    _box(reqs, f"{sid}_c", sid, MARGIN + 6, cust_top, 500, 36, customer)
    _style(reqs, f"{sid}_c", 0, len(customer), size=24, color=MINT, font=FONT, bold=True)

    # Date
    date_text = generated
    _box(reqs, f"{sid}_d", sid, MARGIN + 6, cust_top + 42, 500, 28, date_text)
    _style(reqs, f"{sid}_d", 0, len(date_text), size=19, color=MINT, font=FONT)

    # Confidential footer
    footer = "Proprietary & Confidential"
    _box(reqs, f"{sid}_f", sid, SLIDE_W - 220, SLIDE_H - 28, 200, 16, footer)
    _style(reqs, f"{sid}_f", 0, len(footer), size=8, color=GRAY, font=FONT)

    return idx + 1


def _qbr_agenda_slide(reqs, sid, report, idx):
    """Numbered agenda slide generated from the deck's slide plan."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    # Accent rounded rectangle on the right half
    _rect(reqs, f"{sid}_accent", sid, SLIDE_W * 0.48, 0, SLIDE_W * 0.52, SLIDE_H, _BESPOKE_NAVY)

    # Title
    _box(reqs, f"{sid}_t", sid, MARGIN, MARGIN, 300, 50, "Agenda")
    _style(reqs, f"{sid}_t", 0, len("Agenda"), size=38, color=WHITE, font=FONT_SERIF)

    # Build agenda items from the slide plan.
    # Prefer divider titles (section headings). Fall back to non-structural slide titles.
    slide_plan = report.get("_slide_plan", [])
    divider_items = [
        entry.get("title", "")
        for entry in slide_plan
        if entry.get("slide_type", entry.get("id", "")) == "qbr_divider"
        and entry.get("title")
    ]
    if divider_items:
        items = divider_items
    else:
        skip_types = {"qbr_cover", "qbr_agenda", "title", "data_quality", "skip"}
        items = [
            entry.get("title", entry.get("id", "").replace("_", " ").title())
            for entry in slide_plan
            if entry.get("slide_type", entry.get("id", "")) not in skip_types
        ]

    # Render numbered list — dynamically size to fit
    x = SLIDE_W * 0.52
    y_start = MARGIN + 20
    avail_h = SLIDE_H - MARGIN * 2 - 20
    n_items = len(items)
    line_h = max(28, min(42, avail_h // max(n_items, 1)))
    font_sz = 18 if n_items > 8 else 20
    num_sz = 20 if n_items > 8 else 22
    max_items = min(n_items, avail_h // line_h)

    y = y_start
    for i, item in enumerate(items[:max_items]):
        num = f"{i + 1:02d}"
        label = item[:50] + "…" if len(item) > 50 else item
        _box(reqs, f"{sid}_n{i}", sid, x, y, 40, line_h, num)
        _style(reqs, f"{sid}_n{i}", 0, len(num), size=num_sz, color=MINT, font=FONT, bold=True)

        _box(reqs, f"{sid}_i{i}", sid, x + 48, y, 280, line_h, label)
        _style(reqs, f"{sid}_i{i}", 0, len(label), size=font_sz, color=WHITE, font=FONT)
        y += line_h

    return idx + 1


def _qbr_divider_slide(reqs, sid, report, idx):
    """Section divider slide with LeanDNA tagline and section title."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    # Read section title from the current slide definition
    entry = report.get("_current_slide", {})
    section_title = entry.get("title", entry.get("note", ""))

    # Stacked tagline (left side, large)
    lines = [
        ("THE RIGHT PART.", 28, True),
        ("In the right place.", 28, False),
        ("AT THE RIGHT TIME.", 26, False),
    ]
    ty = SLIDE_H * 0.18
    for li, (text, size, bold) in enumerate(lines):
        _box(reqs, f"{sid}_tl{li}", sid, MARGIN, ty, 400, 36, text)
        _style(reqs, f"{sid}_tl{li}", 0, len(text), size=size, color=WHITE, font=FONT, bold=bold)
        ty += 40

    # Section title (prominent, centered-lower)
    if section_title:
        _box(reqs, f"{sid}_sec", sid, MARGIN, SLIDE_H * 0.65, CONTENT_W, 50, section_title)
        _style(reqs, f"{sid}_sec", 0, len(section_title), size=32, color=MINT, font=FONT_SERIF)

    # Confidential footer
    footer = "Proprietary & Confidential"
    _box(reqs, f"{sid}_f", sid, SLIDE_W - 220, SLIDE_H - 28, 200, 16, footer)
    _style(reqs, f"{sid}_f", 0, len(footer), size=8, color=GRAY, font=FONT)

    return idx + 1


def _qbr_deployment_slide(reqs, sid, report, idx):
    """Deployment overview: site count and status table from Pendo data."""
    all_sites = report.get("sites", [])
    if not all_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo site list for deployment summary")

    customer = report.get("customer", report.get("account", {}).get("customer", ""))
    raw_gen = report.get("generated", "")
    try:
        generated = datetime.datetime.strptime(raw_gen, "%Y-%m-%d").strftime("%B %-d, %Y")
    except (ValueError, TypeError):
        generated = raw_gen or datetime.datetime.now().strftime("%B %-d, %Y")
    subtitle = f"As of {generated}"

    cs_health = get_csr_section(report).get("platform_health") or {}
    site_health = {}
    for row in cs_health.get("sites", []):
        name = row.get("site", "")
        status = row.get("health_status", "")
        if name and status:
            site_health[name.lower()] = status

    customer_prefix = customer.strip()

    def _short_site(name: str) -> str:
        n = name
        if customer_prefix and n.lower().startswith(customer_prefix.lower()):
            n = n[len(customer_prefix):].lstrip(" -·")
        return n[:25] if len(n) > 25 else n

    headers = ["Site", "Users", "Status", "Last Active"]
    col_widths = [220, 60, 80, 130]
    ROW_H = 26
    max_rows = max(1, (BODY_BOTTOM - (BODY_Y + 14)) // ROW_H - 1)
    site_chunks = _cap_chunk_list(
        [all_sites[i : i + max_rows] for i in range(0, len(all_sites), max_rows)]
    )
    status_colors = {
        "GREEN": {"red": 0.1, "green": 0.6, "blue": 0.2},
        "YELLOW": {"red": 0.9, "green": 0.7, "blue": 0.1},
        "RED": {"red": 0.85, "green": 0.15, "blue": 0.15},
    }
    oids: list[str] = []

    for pi, sites_to_show in enumerate(site_chunks):
        page_sid = f"{sid}_p{pi}" if len(site_chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        ttl = "Deployment — Number of Sites" if len(site_chunks) == 1 else f"Deployment — Sites ({pi + 1} of {len(site_chunks)})"
        _slide_title(reqs, page_sid, ttl)
        _box(reqs, f"{page_sid}_sub", page_sid, MARGIN, BODY_Y - 10, CONTENT_W, 18, subtitle)
        _style(reqs, f"{page_sid}_sub", 0, len(subtitle), size=10, color=GRAY, font=FONT)

        rows_data = []
        for s in sites_to_show:
            site_name = _short_site(s.get("sitename", "?"))
            visitors = str(s.get("visitors", 0))
            health = site_health.get(s.get("sitename", "").lower(), "—")
            last_active_raw = s.get("last_active", "—")
            try:
                last_active = datetime.datetime.strptime(
                    str(last_active_raw)[:10], "%Y-%m-%d"
                ).strftime("%b %-d, %Y")
            except (ValueError, TypeError):
                last_active = str(last_active_raw)[:10] if last_active_raw else "—"
            rows_data.append([site_name, visitors, health, last_active])

        tbl_id = f"{page_sid}_tbl"
        _simple_table(reqs, tbl_id, page_sid, MARGIN, BODY_Y + 14,
                      col_widths, ROW_H, headers, rows_data)
        for ri, row in enumerate(rows_data):
            status = row[2].upper() if len(row) > 2 else ""
            if status in status_colors:
                _table_cell_bg(reqs, tbl_id, ri + 1, 2, status_colors[status])

    return idx + len(site_chunks), oids


def _support_breakdown_slide(reqs, sid, report, idx):
    """Engineering-focused support breakdown: weekly trend, TTFR/TTR deep-dive, priority/type table, escalations."""
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(reqs, sid, report, idx, "Jira support ticket data")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Support Breakdown")

    days = jira.get("days", 90)
    total = jira["total_issues"]
    open_n = jira["open_issues"]
    resolved = jira["resolved_issues"]
    esc = jira["escalated"]
    bugs = jira["open_bugs"]
    jira_base = jira.get("base_url", "")

    from datetime import date, timedelta
    end_d = date.today()
    start_d = end_d - timedelta(days=days)
    date_range = f"{start_d.strftime('%b %-d')} – {end_d.strftime('%b %-d, %Y')}  ({days}d)"

    # ── Top stat bar ──
    stats_text = (
        f"Total: {total}   |   Open: {open_n}   |   Resolved: {resolved}"
        f"   |   Escalated: {esc}   |   Open bugs: {bugs}   |   {date_range}"
    )
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 18, stats_text)
    _style(reqs, f"{sid}_bar", 0, len(stats_text), size=9, color=GRAY, font=FONT)
    # Bold the numbers
    for label in (f"Total: {total}", f"Open: {open_n}", f"Resolved: {resolved}",
                  f"Escalated: {esc}", f"Open bugs: {bugs}"):
        pos = stats_text.find(label)
        if pos >= 0:
            _style(reqs, f"{sid}_bar", pos, pos + len(label), bold=True, color=NAVY)

    body_top = BODY_Y + 22
    col_gap = 20
    left_w = (CONTENT_W - col_gap) * 2 // 3
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    # ── LEFT: Weekly trend sparkline + SLA deep-dive ──
    left_y = body_top

    weeks = jira.get("tickets_over_time", [])
    if weeks:
        trend_title = "Weekly ticket volume"
        _box(reqs, f"{sid}_trt", sid, left_x, left_y, left_w, 16, trend_title)
        _style(reqs, f"{sid}_trt", 0, len(trend_title), bold=True, size=11, color=NAVY, font=FONT)
        left_y += 18

        # Sparkline: text bar chart using unicode blocks
        max_created = max((w["created"] for w in weeks), default=1) or 1
        BLOCKS = " ▁▂▃▄▅▆▇█"
        spark_parts = []
        label_parts = []
        for w in weeks[-12:]:  # last 12 weeks
            lvl = int(w["created"] / max_created * 8)
            spark_parts.append(BLOCKS[lvl])
            label_parts.append(w["label"])
        sparkline = "  ".join(spark_parts)
        _box(reqs, f"{sid}_spark", sid, left_x, left_y, left_w, 22, sparkline)
        _style(reqs, f"{sid}_spark", 0, len(sparkline), size=16, color=BLUE, font="Courier New")
        left_y += 24

        # Labels under sparkline
        label_text = "   ".join(w["label"] for w in weeks[-12:])
        _box(reqs, f"{sid}_sparklbl", sid, left_x, left_y, left_w, 12, label_text)
        _style(reqs, f"{sid}_sparklbl", 0, len(label_text), size=7, color=GRAY, font=FONT)
        left_y += 18
    else:
        left_y += 4

    # SLA section
    sla_goal = {"ttfr": "48h", "ttr": "160h"}
    sla_label_map = {"ttfr": "First Response (TTFR)", "ttr": "Resolution (TTR)"}
    for sla_key in ("ttfr", "ttr"):
        sla = jira.get(sla_key, {})
        if sla.get("measured", 0) == 0:
            continue
        label = sla_label_map[sla_key]
        goal = sla_goal[sla_key]
        breached = sla.get("breached", 0)
        measured = sla.get("measured", 1)
        breach_pct = round(100 * breached / max(measured, 1))

        if breach_pct == 0:
            b_color: dict = {"red": 0.13, "green": 0.55, "blue": 0.13}
            b_label = "On track"
        elif breach_pct <= 20:
            b_color = {"red": 0.85, "green": 0.65, "blue": 0.0}
            b_label = f"Caution ({breach_pct}%)"
        else:
            b_color = {"red": 0.85, "green": 0.15, "blue": 0.15}
            b_label = f"At risk ({breach_pct}%)"

        title_text = f"{label}  ·  goal {goal}"
        _box(reqs, f"{sid}_{sla_key}_t", sid, left_x, left_y, left_w, 18, title_text)
        _style(reqs, f"{sid}_{sla_key}_t", 0, len(label), bold=True, size=12, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_{sla_key}_t", len(label), len(title_text), size=9, color=GRAY, font=FONT)
        left_y += 20
        _pill(reqs, f"{sid}_{sla_key}_pill", sid, left_x, left_y, 110, 20, b_label, b_color, WHITE)
        stats_sla = (
            f"Median {sla.get('median', '—')}  ·  Avg {sla.get('avg', '—')}"
            f"  ·  Range {sla.get('min', '—')}–{sla.get('max', '—')}"
            f"  ·  {breached} breaches of {measured}"
        )
        if sla.get("waiting"):
            stats_sla += f"  ·  {sla['waiting']} open"
        _box(reqs, f"{sid}_{sla_key}_st", sid, left_x + 118, left_y, left_w - 120, 20, stats_sla)
        _style(reqs, f"{sid}_{sla_key}_st", 0, len(stats_sla), size=9, color=NAVY, font=FONT)
        left_y += 26

    # ── RIGHT: Priority + Type + Escalations ──
    right_y = body_top

    prio_short = {
        "Blocker: The platform is completely down": "Blocker",
        "Critical: Significant operational impact": "Critical",
        "Major: Workaround available, not essential": "Major",
        "Minor: Impairs non-essential functionality": "Minor",
    }
    prio_items = list(jira.get("by_priority", {}).items())[:6]
    if prio_items:
        ph = "By Priority"
        _box(reqs, f"{sid}_prt", sid, right_x, right_y, right_w, 16, ph)
        _style(reqs, f"{sid}_prt", 0, len(ph), bold=True, size=11, color=NAVY, font=FONT)
        right_y += 18
        for pi, (p, c) in enumerate(prio_items):
            line = f"{c:>4}  {prio_short.get(p, p[:22])}"
            _box(reqs, f"{sid}_pr{pi}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_pr{pi}", 0, len(line), size=9, color=NAVY, font=FONT)
            _style(reqs, f"{sid}_pr{pi}", 0, len(f"{c:>4}"), bold=True, color=BLUE)
            right_y += 14
        right_y += 8

    type_items = list(jira.get("by_type", {}).items())[:6]
    if type_items:
        th = "By Type"
        _box(reqs, f"{sid}_tyt", sid, right_x, right_y, right_w, 16, th)
        _style(reqs, f"{sid}_tyt", 0, len(th), bold=True, size=11, color=NAVY, font=FONT)
        right_y += 18
        for ti, (tp, c) in enumerate(type_items):
            line = f"{c:>4}  {tp[:22]}"
            _box(reqs, f"{sid}_ty{ti}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_ty{ti}", 0, len(line), size=9, color=NAVY, font=FONT)
            _style(reqs, f"{sid}_ty{ti}", 0, len(f"{c:>4}"), bold=True, color=BLUE)
            right_y += 14
        right_y += 8

    esc_issues = jira.get("escalated_issues", [])
    if esc_issues and right_y + 40 < BODY_BOTTOM:
        eh = "Escalated Tickets"
        _box(reqs, f"{sid}_esct", sid, right_x, right_y, right_w, 16, eh)
        _style(reqs, f"{sid}_esct", 0, len(eh), bold=True, size=11, color=_RED, font=FONT)
        right_y += 18
        for ei, esc_i in enumerate(esc_issues[:4]):
            if right_y + 14 > BODY_BOTTOM:
                break
            key = esc_i["key"]
            summary = esc_i.get("summary", "")[:28]
            line = f"{key}  {summary}"
            link = f"{jira_base}/browse/{key}" if jira_base else None
            _box(reqs, f"{sid}_esc{ei}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_esc{ei}", 0, len(key), bold=True, size=9, color=_RED, font=MONO,
                   link=link)
            _style(reqs, f"{sid}_esc{ei}", len(key) + 2, len(line), size=9, color=NAVY, font=FONT)
            right_y += 14

    return idx + 1


# ── Engineering Portfolio Slides ──────────────────────────────────────────────

def _eng_insight_bullets(reqs, sid, bullets: list[str], x, y, w) -> int:
    """Render 2-3 LeanDNA insight bullets. Returns new y position."""
    if not bullets:
        return y
    for bi, bullet in enumerate(bullets[:3]):
        text = f"· {bullet}"
        _box(reqs, f"{sid}_ins{bi}", sid, x, y, w, 22, text)
        _style(reqs, f"{sid}_ins{bi}", 0, 2, bold=True, size=9, color=BLUE, font=FONT)
        _style(reqs, f"{sid}_ins{bi}", 2, len(text), size=9, color=NAVY, font=FONT)
        y += 22
    return y


def _eng_portfolio_title_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Cover slide for the engineering portfolio deck."""
    from datetime import date
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    title = "Engineering Review"
    _box(reqs, f"{sid}_t", sid, MARGIN, 100, CONTENT_W, 50, title)
    _style(reqs, f"{sid}_t", 0, len(title), bold=True, size=36, color=WHITE, font=FONT)

    eng = report.get("eng_portfolio") or {}
    sprint = eng.get("sprint") or {}
    sprint_name = sprint.get("name", "")
    sprint_end = sprint.get("end", "")
    try:
        from datetime import datetime
        end_dt = datetime.strptime(sprint_end, "%Y-%m-%d")
        sprint_label = f"{sprint_name}  ·  ends {end_dt.strftime('%b %-d, %Y')}"
    except Exception:
        sprint_label = sprint_name or ""

    sub = f"Sprint: {sprint_label}" if sprint_label else ""
    if sub:
        _box(reqs, f"{sid}_sp", sid, MARGIN, 160, CONTENT_W, 24, sub)
        _style(reqs, f"{sid}_sp", 0, len(sub), size=14, color={"red": 0.6, "green": 0.8, "blue": 1.0}, font=FONT)

    generated = date.today().strftime("%B %-d, %Y")
    gen_text = f"Generated {generated}"
    _box(reqs, f"{sid}_g", sid, MARGIN, SLIDE_H - 60, CONTENT_W, 18, gen_text)
    _style(reqs, f"{sid}_g", 0, len(gen_text), size=10, color={"red": 0.5, "green": 0.6, "blue": 0.7}, font=FONT)
    return idx + 1


def _eng_sprint_snapshot_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Sprint snapshot: current sprint state, type mix, active work by theme."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    sprint = eng.get("sprint") or {}
    sprint_name = sprint.get("name", "Current Sprint")
    sprint_start = sprint.get("start", "")
    sprint_end = sprint.get("end", "")
    try:
        from datetime import datetime
        s_dt = datetime.strptime(sprint_start, "%Y-%m-%d")
        e_dt = datetime.strptime(sprint_end, "%Y-%m-%d")
        date_range = f"{s_dt.strftime('%b %-d')} – {e_dt.strftime('%b %-d, %Y')}"
    except Exception:
        date_range = f"{sprint_start} – {sprint_end}"

    in_f = eng.get("in_flight_count", 0)
    closed = eng.get("closed_count", 0)
    by_status = eng.get("by_status", {})
    active = by_status.get("In Progress", 0) + by_status.get("In Review", 0)
    by_type = eng.get("by_type", {})
    bugs_if = by_type.get("Bug", 0)

    # Dynamic insight title
    title = f"{sprint_name}: {in_f} Open, {active} Active, {bugs_if} Bugs"
    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    # Scope context line
    ctx = f"{date_range}   ·   Closed this period: {closed}"
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, ctx)
    _style(reqs, f"{sid}_ctx", 0, len(ctx), size=9, color=GRAY, font=FONT)

    body_top = BODY_Y + 18
    col_gap = 24
    left_w = (CONTENT_W - col_gap) * 3 // 5
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    # ── LEFT: Themes with visual bar (total tickets per theme) ──
    left_y = body_top
    themes = eng.get("themes", [])

    th_hdr = "Work In Progress — by Theme"
    _box(reqs, f"{sid}_tht", sid, left_x, left_y, left_w, 16, th_hdr)
    _style(reqs, f"{sid}_tht", 0, len(th_hdr), bold=True, size=11, color=NAVY, font=FONT)
    left_y += 18

    max_theme_total = max((t["total"] for t in themes), default=1) or 1
    BAR_MAX = left_w - 120  # px available for the bar
    ROW_H = 16

    for ri, th in enumerate(themes[:14]):
        if left_y + ROW_H > BODY_BOTTOM - 80:
            break
        theme_name = th["theme"][:24]
        total_n = th["total"]
        active_n = th["in_progress"]
        bugs_n = th["bugs"]

        bar_w = max(4, int(total_n / max_theme_total * BAR_MAX))

        # Label: name + counts
        label = f"{theme_name}"
        counts = f"{total_n}" + (f" ({active_n} act)" if active_n else "") + (f" {bugs_n}B" if bugs_n else "")
        _box(reqs, f"{sid}_tln{ri}", sid, left_x, left_y, 96, ROW_H, label)
        _style(reqs, f"{sid}_tln{ri}", 0, len(label), size=8, color=NAVY, font=FONT)

        # Visual bar — capped so the count label fits within the left column
        bar_x = left_x + 100
        max_bar_w = left_w - 100 - 52 - 4  # leave 52px for count label
        bar_w_capped = min(bar_w, max_bar_w)
        bar_color = {"red": 0.9, "green": 0.4, "blue": 0.0} if bugs_n else BLUE
        _box(reqs, f"{sid}_tbar{ri}", sid, bar_x, left_y + 4, bar_w_capped, 9, "")
        reqs.append({"updateShapeProperties": {
            "objectId": f"{sid}_tbar{ri}",
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": bar_color}}},
                "outline": {
                    "outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                    "weight": {"magnitude": 0.75, "unit": "PT"},
                },
            },
            "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
        }})

        # Count label after bar
        _box(reqs, f"{sid}_tcnt{ri}", sid, bar_x + bar_w_capped + 4, left_y, 48, ROW_H, counts)
        _style(reqs, f"{sid}_tcnt{ri}", 0, len(counts), size=8,
               color=_RED if bugs_n else GRAY, font=FONT)
        left_y += ROW_H

    charts = report.get("_charts")

    # ── RIGHT top: Type mix ──
    right_y = body_top
    if by_type:
        _box(reqs, f"{sid}_typ_h", sid, right_x, right_y, right_w, 14, "Type Mix")
        _style(reqs, f"{sid}_typ_h", 0, 8, bold=True, size=10, color=NAVY, font=FONT)
        right_y += 16
        if charts:
            from .charts import embed_chart
            type_items = list(by_type.items())[:6]
            ss_id, chart_id = charts.add_bar_chart(
                title="Type Mix",
                labels=[tp for tp, _ in type_items],
                series={"Open tickets": [cnt for _, cnt in type_items]},
                horizontal=False,
            )
            embed_chart(
                reqs, f"{sid}_type_mix", sid, ss_id, chart_id,
                right_x, right_y, right_w, 120, linked=False,
            )
            right_y += 126

    # ── RIGHT mid: WIP by engineer (vertical bars) ──
    by_assignee = eng.get("by_assignee", {})
    top_assignees = sorted(by_assignee.items(), key=lambda x: -x[1])[:7]
    if top_assignees:
        _box(reqs, f"{sid}_ass_h", sid, right_x, right_y, right_w, 14, "WIP by Engineer")
        _style(reqs, f"{sid}_ass_h", 0, 15, bold=True, size=10, color=NAVY, font=FONT)
        right_y += 16
        if charts:
            from .charts import embed_chart
            ss_id, chart_id = charts.add_bar_chart(
                title="WIP by Engineer",
                labels=[(name.split()[0] if name else "Unassigned") for name, _ in top_assignees],
                series={"Open tickets": [cnt for _, cnt in top_assignees]},
                horizontal=False,
            )
            embed_chart(
                reqs, f"{sid}_wip_eng", sid, ss_id, chart_id,
                right_x, right_y, right_w, 120, linked=False,
            )
            right_y += 126

    # ── INSIGHT BULLETS (bottom of slide) ──
    insights = (eng.get("insights") or {}).get("sprint_snapshot", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        _eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


def _eng_bug_health_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Bug health: open bugs by priority, blocker/critical callout, trend."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    open_bugs_list = eng.get("open_bugs") or []
    blocker_crit = eng.get("blocker_critical") or []

    # Dynamic insight title
    if blocker_crit:
        title = f"{len(open_bugs_list)} Open Bugs — {len(blocker_crit)} Blocker/Critical Need Attention"
    elif open_bugs_list:
        title = f"{len(open_bugs_list)} Open Bugs — No Blockers Currently Active"
    else:
        title = "Bug Backlog Clear — No Open Bugs"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    open_bugs = eng.get("open_bugs") or []
    blocker_crit = eng.get("blocker_critical") or []
    jira_base = eng.get("base_url", "")

    # Stat bar
    bar = f"Open bugs: {len(open_bugs)}   |   Blocker / Critical: {len(blocker_crit)}"
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 18, bar)
    _style(reqs, f"{sid}_bar", 0, len(bar), size=9, color=GRAY, font=FONT)
    # Bold the counts
    _style(reqs, f"{sid}_bar", len("Open bugs: "), len(f"Open bugs: {len(open_bugs)}"),
           bold=True, color=_RED if open_bugs else _GREEN)
    bc_start = bar.index("Blocker")
    _style(reqs, f"{sid}_bar", bc_start, bc_start + len(f"Blocker / Critical: {len(blocker_crit)}"),
           bold=True, color=_RED if blocker_crit else _GREEN)

    body_top = BODY_Y + 22
    col_gap = 20
    left_w = (CONTENT_W - col_gap) * 2 // 3
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    # ── LEFT: Open bugs list ──
    left_y = body_top
    _box(reqs, f"{sid}_bl_h", sid, left_x, left_y, left_w, 16, "Open Bugs")
    _style(reqs, f"{sid}_bl_h", 0, 9, bold=True, size=11, color=NAVY, font=FONT)
    left_y += 18

    prio_color = {
        "Blocker": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "Critical": {"red": 0.9, "green": 0.4, "blue": 0.0},
        "Major": NAVY,
        "Minor": GRAY,
    }
    TICKET_H = 34  # key line (16) + summary line (18)
    for bi, bug in enumerate(open_bugs[:12]):
        if left_y + TICKET_H > BODY_BOTTOM - 72:  # reserve space for insight bullets
            break
        key = bug["key"]
        prio_short = bug["priority"].split(":")[0] if ":" in bug["priority"] else bug["priority"]
        assignee = (bug.get("assignee") or "")
        first_name = assignee.split()[0] if assignee else "—"
        raw_summary = bug["summary"]
        summary = raw_summary[:48] + "…" if len(raw_summary) > 48 else raw_summary

        key_line = f"{key}  [{prio_short}]  {first_name}"
        link = f"{jira_base}/browse/{key}" if jira_base else None
        _box(reqs, f"{sid}_bk{bi}", sid, left_x, left_y, left_w, 16, key_line)
        _style(reqs, f"{sid}_bk{bi}", 0, len(key), bold=True, size=8,
               color=prio_color.get(prio_short, _RED), font=MONO, link=link)
        _style(reqs, f"{sid}_bk{bi}", len(key), len(key_line), size=8, color=GRAY, font=FONT)
        left_y += 16

        _box(reqs, f"{sid}_bs{bi}", sid, left_x + 8, left_y, left_w - 8, 16, summary)
        _style(reqs, f"{sid}_bs{bi}", 0, len(summary), size=8, color=NAVY, font=FONT)
        left_y += 18

    # ── RIGHT: Priority breakdown + escalated bugs ──
    right_y = body_top
    by_type = eng.get("by_type", {})
    bug_count = by_type.get("Bug", 0)

    # Priority distribution of ALL open tickets (not just bugs)
    by_prio: dict[str, int] = {}
    for bug in (eng.get("open_bugs") or []):
        short = bug["priority"].split(":")[0] if ":" in bug["priority"] else bug["priority"]
        by_prio[short] = by_prio.get(short, 0) + 1

    if by_prio:
        _box(reqs, f"{sid}_ph", sid, right_x, right_y, right_w, 16, "By Priority")
        _style(reqs, f"{sid}_ph", 0, 11, bold=True, size=11, color=NAVY, font=FONT)
        right_y += 18
        for pi, (p, c) in enumerate(sorted(by_prio.items(),
                                            key=lambda x: ["Blocker","Critical","Major","Minor"].index(x[0])
                                            if x[0] in ["Blocker","Critical","Major","Minor"] else 99)):
            line = f"{c:>4}  {p}"
            _box(reqs, f"{sid}_pp{pi}", sid, right_x, right_y, right_w, 13, line)
            col = prio_color.get(p, NAVY)
            _style(reqs, f"{sid}_pp{pi}", 0, len(f"{c:>4}"), bold=True, size=10, color=col, font=FONT)
            _style(reqs, f"{sid}_pp{pi}", len(f"{c:>4}"), len(line), size=10, color=NAVY, font=FONT)
            right_y += 14
        right_y += 10

    if blocker_crit:
        _box(reqs, f"{sid}_bch", sid, right_x, right_y, right_w, 16, "Blockers & Criticals")
        _style(reqs, f"{sid}_bch", 0, 20, bold=True, size=11, color=_RED, font=FONT)
        right_y += 18
        for bi, bug in enumerate(blocker_crit[:6]):
            key = bug["key"]
            link = f"{jira_base}/browse/{key}" if jira_base else None
            raw_s = bug["summary"]
            summary = raw_s[:30] + "…" if len(raw_s) > 30 else raw_s
            line = f"{key}  {summary}"
            _box(reqs, f"{sid}_bc{bi}", sid, right_x, right_y, right_w, 16, line)
            _style(reqs, f"{sid}_bc{bi}", 0, len(key), bold=True, size=9,
                   color=_RED, font=MONO, link=link)
            _style(reqs, f"{sid}_bc{bi}", len(key), len(line), size=9, color=NAVY, font=FONT)
            right_y += 17

    # ── INSIGHT BULLETS ──
    insights = (eng.get("insights") or {}).get("bug_health", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        _eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


def _eng_velocity_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Velocity & throughput: combo chart (bars=Created, line=Closed) + pipeline status."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    throughput = eng.get("throughput") or []
    closed_count = eng.get("closed_count", 0)
    in_flight = eng.get("in_flight_count", 0)

    # Dynamic insight title based on flow balance
    recent_tp = throughput[-4:] if throughput else []
    avg_closed = sum(w.get("resolved", 0) for w in recent_tp) / len(recent_tp) if recent_tp else 0
    avg_created = sum(w.get("created", 0) for w in recent_tp) / len(recent_tp) if recent_tp else 0
    net = avg_closed - avg_created
    if net > 2:
        title = f"Backlog Shrinking — {net:.0f} More Tickets Closed Than Created Per Week"
    elif net < -2:
        title = f"Backlog Growing — {abs(net):.0f} More Created Than Closed Per Week"
    else:
        title = f"Flow Balanced — Averaging {avg_closed:.0f} Tickets Closed Per Week"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    ctx = f"Open: {in_flight}   ·   Closed this period: {closed_count}   ·   Last 12 weeks"
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 14, ctx)
    _style(reqs, f"{sid}_bar", 0, len(ctx), size=9, color=GRAY, font=FONT)

    body_top = BODY_Y + 22
    col_gap = 20
    left_w = (CONTENT_W - col_gap) * 3 // 5
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    # ── LEFT: Weekly throughput combo chart ──
    left_y = body_top
    recent_weeks = throughput[-12:] if len(throughput) >= 12 else throughput
    charts = report.get("_charts")
    if recent_weeks and charts:
        try:
            from .charts import embed_chart

            ss_id, chart_id = charts.add_combo_chart(
                title="Weekly Throughput",
                labels=[w.get("label", "") for w in recent_weeks],
                bar_series={"Created": [w.get("created", 0) for w in recent_weeks]},
                line_series={"Closed": [w.get("resolved", 0) for w in recent_weeks]},
            )
            embed_chart(
                reqs, f"{sid}_chart", sid, ss_id, chart_id,
                left_x, left_y, left_w, 170, linked=False,
            )
            left_y += 176
        except Exception as e:
            logger.warning("Throughput chart embed failed: %s", e)

    # Weekly data table below chart (last 8 weeks)
    if recent_weeks:
        left_y += 4
        _box(reqs, f"{sid}_wt_h", sid, left_x, left_y, left_w, 14, "Week        Created  Closed")
        _style(reqs, f"{sid}_wt_h", 0, len("Week        Created  Closed"), bold=True, size=8, color=GRAY, font=MONO)
        left_y += 14
        for w in recent_weeks[-8:]:
            row = f"{w['label']:<12}  {w.get('created',0):>5}    {w.get('resolved',0):>4}"
            _box(reqs, f"{sid}_wr{w['week']}", sid, left_x, left_y, left_w, 12, row)
            _style(reqs, f"{sid}_wr{w['week']}", 0, len(row), size=8, color=NAVY, font=MONO)
            left_y += 12

    # ── RIGHT: Q-label goal tracking ──
    right_y = body_top
    _box(reqs, f"{sid}_qlh", sid, right_x, right_y, right_w, 16, "Quarterly Goal Tracking")
    _style(reqs, f"{sid}_qlh", 0, 24, bold=True, size=11, color=NAVY, font=FONT)
    right_y += 20

    # Look at in-flight tickets with Q labels
    by_type = eng.get("by_type", {})
    in_flight_total = sum(by_type.values())

    # Q-label stats from themes (tickets labeled Q1_2026 / Q2_2026)
    q_labels = {"Q1_2026": {"in_flight": 0, "closed": 0},
                "Q2_2026": {"in_flight": 0, "closed": 0}}

    for theme_entry in (eng.get("themes") or []):
        for t in theme_entry.get("tickets", []):
            pass  # themes don't carry full label data

    # Use support data as proxy context
    sp_pressure = eng.get("support_pressure") or {}
    sp_total = sp_pressure.get("total", 0)
    sp_open = sp_pressure.get("open", 0)
    sp_esc = sp_pressure.get("escalated_to_eng", 0)

    # Status breakdown
    by_status = eng.get("by_status") or {}
    stat_items = sorted(by_status.items(), key=lambda x: -x[1])

    _box(reqs, f"{sid}_sbh", sid, right_x, right_y, right_w, 14, "Pipeline Status")
    _style(reqs, f"{sid}_sbh", 0, 15, bold=True, size=10, color=NAVY, font=FONT)
    right_y += 16
    total_if = sum(by_status.values()) or 1
    max_s = max(by_status.values()) if by_status else 1
    PCT_COL_W = 30  # fixed width column for the % label
    BAR_MAX_W = right_w - 76 - PCT_COL_W - 4
    for status, cnt in stat_items:
        pct = int(cnt / total_if * 100)
        bw = max(3, int(cnt / max_s * BAR_MAX_W))
        safe_status = status.replace(" ", "_").replace("/", "_")[:10]
        is_active = status in ("In Progress", "In Review")
        bar_color = BLUE if is_active else {"red": 0.75, "green": 0.80, "blue": 0.90}
        label = f"{cnt}  {status}"
        _box(reqs, f"{sid}_sl_{safe_status}", sid, right_x, right_y, 70, 13, label)
        _style(reqs, f"{sid}_sl_{safe_status}", 0, len(str(cnt)), bold=is_active, size=8,
               color=BLUE if is_active else NAVY, font=FONT)
        _style(reqs, f"{sid}_sl_{safe_status}", len(str(cnt)) + 2, len(label), size=8, color=GRAY, font=FONT)
        _box(reqs, f"{sid}_sb_{safe_status}", sid, right_x + 72, right_y + 3, bw, 8, "")
        reqs.append({"updateShapeProperties": {
            "objectId": f"{sid}_sb_{safe_status}",
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": bar_color}}},
                "outline": {
                    "outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                    "weight": {"magnitude": 0.75, "unit": "PT"},
                },
            },
            "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
        }})
        pct_lbl = f"{pct}%"
        pct_x = right_x + right_w - PCT_COL_W
        _box(reqs, f"{sid}_sp_{safe_status}", sid, pct_x, right_y, PCT_COL_W, 13, pct_lbl)
        _style(reqs, f"{sid}_sp_{safe_status}", 0, len(pct_lbl), size=8, color=GRAY, font=FONT)
        right_y += 14

    # ── INSIGHT BULLETS ──
    insights = (eng.get("insights") or {}).get("velocity", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        _eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


def _eng_enhancements_open_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Open enhancement requests — paginated, all tickets shown."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    enhancements = eng.get("enhancements") or {}
    open_tickets = enhancements.get("open", [])
    open_count = enhancements.get("open_count", 0)
    shipped_count = enhancements.get("shipped_count", 0)
    declined_count = enhancements.get("declined_count", 0)
    days = enhancements.get("days", eng.get("days", 30))
    jira_base = eng.get("base_url", "")

    TICKETS_PER_PAGE = 3
    pages_all = [open_tickets[i:i + TICKETS_PER_PAGE]
                 for i in range(0, max(1, len(open_tickets)), TICKETS_PER_PAGE)]
    pages = _cap_chunk_list(pages_all)
    num_pages = len(pages)
    omitted_tickets = sum(len(p) for p in pages_all[len(pages):])

    for pg, page_tickets in enumerate(pages):
        page_sid = f"{sid}_p{pg}"
        if pg == 0:
            title = (f"{open_count} Open Enhancement Request  ({pg + 1} of {num_pages})"
                     if num_pages > 1 else (
                         f"1 Open Enhancement Request in Backlog" if open_count == 1
                         else f"{open_count} Open Enhancement Requests in Backlog"
                     ))
        else:
            title = f"Enhancement Requests — Open  ({pg + 1} of {num_pages})"

        _slide(reqs, page_sid, idx)
        _bg(reqs, page_sid, WHITE)
        _slide_title(reqs, page_sid, title)

        bar = (f"Open backlog: {open_count}   |   Recently shipped: {shipped_count}"
               f"   |   Declined: {declined_count}")
        _box(reqs, f"{page_sid}_bar", page_sid, MARGIN, BODY_Y, CONTENT_W, 18, bar)
        _style(reqs, f"{page_sid}_bar", 0, len(bar), size=9, color=GRAY, font=FONT)

        y = BODY_Y + 22
        for ri, req in enumerate(page_tickets):
            key = req["key"]
            link = f"{jira_base}/browse/{key}" if jira_base else None
            raw_summary = req["summary"]
            summary = raw_summary[:87] + "…" if len(raw_summary) > 87 else raw_summary
            status = req.get("status", "Open")

            # Format date nicely
            raw_date = req.get("updated", "")
            try:
                from datetime import datetime as _dt
                updated = _dt.strptime(raw_date, "%Y-%m-%d").strftime("%b %-d, %Y") if raw_date else ""
            except ValueError:
                updated = raw_date

            # Line 1: key + status + date
            meta = f"{key}  [{status}]"
            if updated:
                meta += f"  ·  updated {updated}"
            _box(reqs, f"{page_sid}_k{ri}", page_sid, MARGIN, y, CONTENT_W, 14, meta)
            _style(reqs, f"{page_sid}_k{ri}", 0, len(key), bold=True, size=9,
                   color=BLUE, font=MONO, link=link)
            _style(reqs, f"{page_sid}_k{ri}", len(key), len(meta), size=9, color=GRAY, font=FONT)
            y += 14

            # Line 2: summary (2–3 line box to handle long titles)
            _box(reqs, f"{page_sid}_s{ri}", page_sid, MARGIN + 8, y, CONTENT_W - 8, 36, summary)
            _style(reqs, f"{page_sid}_s{ri}", 0, len(summary), size=9, color=NAVY, font=FONT)
            y += 36

            # Line 3: narrative
            narrative = (req.get("narrative") or "").strip()
            if narrative and y + 40 <= BODY_BOTTOM:
                _box(reqs, f"{page_sid}_n{ri}", page_sid, MARGIN + 8, y, CONTENT_W - 8, 40, narrative)
                _style(reqs, f"{page_sid}_n{ri}", 0, len(narrative), size=8, color=GRAY, font=FONT)
                y += 42

            y += 4

        idx += 1

    if omitted_tickets:
        omit_sid = f"{sid}_omit"
        _slide(reqs, omit_sid, idx)
        _bg(reqs, omit_sid, WHITE)
        _slide_title(reqs, omit_sid, f"Enhancement Requests — Open (continued)")
        note = (f"{omitted_tickets} additional open enhancement requests not shown "
                f"(pagination cap {MAX_PAGINATED_SLIDE_PAGES} pages). "
                f"Full backlog: {open_count} open tickets. View in Jira for complete list.")
        _box(reqs, f"{omit_sid}_note", omit_sid, MARGIN, BODY_Y + 10, CONTENT_W, 40, note)
        _style(reqs, f"{omit_sid}_note", 0, len(note), size=11, color=GRAY, font=FONT)
        idx += 1

    return idx


def _eng_enhancements_shipped_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Recently shipped enhancement requests — what's been delivered."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    enhancements = eng.get("enhancements") or {}
    shipped_count = enhancements.get("shipped_count", 0)
    open_count = enhancements.get("open_count", 0)
    declined_count = enhancements.get("declined_count", 0)

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, f"{shipped_count} Enhancement Requests Recently Shipped")

    bar = (f"Recently shipped: {shipped_count}   |   Open backlog: {open_count}"
           f"   |   Declined: {declined_count}")
    _box(reqs, f"{sid}_bar", sid, MARGIN, BODY_Y, CONTENT_W, 18, bar)
    _style(reqs, f"{sid}_bar", 0, len(bar), size=9, color=GRAY, font=FONT)

    jira_base = eng.get("base_url", "")
    TICKET_H = 96  # meta line (14) + summary box (36) + narrative (42) + gap (4)
    y = BODY_Y + 22

    if not enhancements.get("shipped"):
        msg = ("No enhancement requests were marked as resolved in Jira in the last 12 months. "
               "This may indicate that shipped work isn't being closed out in the ER project — "
               "worth a quick audit of the Jira workflow.")
        _box(reqs, f"{sid}_empty", sid, MARGIN, y + 20, CONTENT_W, 60, msg)
        _style(reqs, f"{sid}_empty", 0, len(msg), size=11, color=GRAY, font=FONT)
        # Flag it visually
        flag = "Action needed: update Jira ER tickets when shipping"
        _box(reqs, f"{sid}_flag", sid, MARGIN, y + 90, CONTENT_W, 20, flag)
        _style(reqs, f"{sid}_flag", 0, len(flag), bold=True, size=10, color=_RED, font=FONT)
        return idx + 1

    for si, req in enumerate(enhancements.get("shipped", [])[:10]):
        if y + TICKET_H > BODY_BOTTOM:
            break
        key = req["key"]
        link = f"{jira_base}/browse/{key}" if jira_base else None
        raw_summary = req["summary"]
        summary = raw_summary[:87] + "…" if len(raw_summary) > 87 else raw_summary
        raw_date = req.get("updated", "")
        try:
            from datetime import datetime as _dt
            updated = _dt.strptime(raw_date, "%Y-%m-%d").strftime("%b %-d, %Y") if raw_date else ""
        except ValueError:
            updated = raw_date

        # Line 1: key + date
        meta = f"{key}  [Shipped]"
        if updated:
            meta += f"  ·  shipped {updated}"
        _box(reqs, f"{sid}_k{si}", sid, MARGIN, y, CONTENT_W, 14, meta)
        _style(reqs, f"{sid}_k{si}", 0, len(key), bold=True, size=9,
               color=_GREEN, font=MONO, link=link)
        _style(reqs, f"{sid}_k{si}", len(key), len(meta), size=9, color=GRAY, font=FONT)
        y += 14

        # Line 2: summary (2–3 line box to handle long titles)
        _box(reqs, f"{sid}_s{si}", sid, MARGIN + 8, y, CONTENT_W - 8, 36, summary)
        _style(reqs, f"{sid}_s{si}", 0, len(summary), size=9, color=NAVY, font=FONT)
        y += 36

        # Line 3: narrative
        narrative = (req.get("narrative") or "").strip()
        if narrative and y + 40 <= BODY_BOTTOM:
            _box(reqs, f"{sid}_n{si}", sid, MARGIN + 8, y, CONTENT_W - 8, 40, narrative)
            _style(reqs, f"{sid}_n{si}", 0, len(narrative), size=8, color=GRAY, font=FONT)
            y += 42

        y += 4

    return idx + 1


def _eng_support_pressure_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Cross-customer support pressure feeding into engineering."""
    eng = report.get("eng_portfolio") or {}
    if not eng:
        return _missing_data_slide(reqs, sid, report, idx, "Engineering portfolio data (Jira LEAN project)")

    sp = eng.get("support_pressure") or {}
    total = sp.get("total", 0)
    open_n = sp.get("open", 0)
    esc = sp.get("escalated_to_eng", 0)
    bugs = sp.get("open_bugs", 0)
    days = eng.get("days", 30)

    # Dynamic insight title
    esc_pct = int(esc / total * 100) if total else 0
    if esc_pct >= 30:
        title = f"{esc_pct}% of Support Tickets Escalated to Engineering — High Pressure"
    elif esc_pct >= 15:
        title = f"{total} Support Tickets — {esc} Escalated to Engineering This Period"
    elif total:
        title = f"{total} Support Tickets — Engineering Escalation Rate at {esc_pct}%"
    else:
        title = "Support Pressure — No Ticket Data Available"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    ctx = f"Last {days} days   ·   Open: {open_n}   ·   Escalated to eng: {esc}   ·   Open bugs: {bugs}"
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 14, ctx)
    _style(reqs, f"{sid}_ctx", 0, len(ctx), size=9, color=GRAY, font=FONT)

    body_top = BODY_Y + 18
    col_gap = 24
    left_w = (CONTENT_W - col_gap) * 3 // 5
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap

    # ── LEFT: Priority breakdown as large horizontal bar chart ──
    by_prio = sp.get("by_priority") or {}
    left_y = body_top
    _box(reqs, f"{sid}_ph", sid, left_x, left_y, left_w, 16, "Ticket Volume by Priority")
    _style(reqs, f"{sid}_ph", 0, 26, bold=True, size=12, color=NAVY, font=FONT)
    left_y += 22

    prio_order = ["Blocker", "Critical", "Major", "Minor", "Unknown"]
    prio_colors = {
        "Blocker": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "Critical": {"red": 0.9, "green": 0.4, "blue": 0.0},
        "Major": BLUE,
        "Minor": {"red": 0.48, "green": 0.77, "blue": 0.98},
        "Unknown": GRAY,
    }
    all_items = [(p, by_prio.get(p, 0)) for p in prio_order if by_prio.get(p, 0) > 0]
    max_val = max(v for _, v in all_items) if all_items else 1
    BAR_MAX_W = left_w - 100

    for pi, (prio, cnt) in enumerate(all_items):
        bar_w = max(6, int(cnt / max_val * BAR_MAX_W))
        is_critical = prio in ("Blocker", "Critical")
        _box(reqs, f"{sid}_pl{pi}", sid, left_x, left_y, 88, 26, prio)
        _style(reqs, f"{sid}_pl{pi}", 0, len(prio), size=12, bold=is_critical,
               color=prio_colors.get(prio, NAVY), font=FONT)
        _box(reqs, f"{sid}_pb{pi}", sid, left_x + 92, left_y + 6, bar_w, 14, "")
        reqs.append({"updateShapeProperties": {
            "objectId": f"{sid}_pb{pi}",
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": prio_colors.get(prio, NAVY)}}},
                "outline": {
                    "outlineFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                    "weight": {"magnitude": 0.75, "unit": "PT"},
                },
            },
            "fields": "shapeBackgroundFill,outline.outlineFill,outline.weight",
        }})
        cnt_lbl = str(cnt)
        _box(reqs, f"{sid}_pc{pi}", sid, left_x + 96 + bar_w, left_y + 4, 40, 18, cnt_lbl)
        _style(reqs, f"{sid}_pc{pi}", 0, len(cnt_lbl), size=11, bold=is_critical,
               color=prio_colors.get(prio, NAVY), font=FONT)
        left_y += 30

    # ── RIGHT: KPI cards (same chrome as all ``_kpi_metric_card`` tiles) ──
    _ENG_SP_KPI_H = 52
    _ENG_SP_KPI_GAP = 6
    right_y = body_top
    kpi_cards = [
        ("Total", total, None),
        ("Open", open_n, None),
        ("Escalated to Eng", esc, _RED if esc > 5 else BLUE),
        ("Open Bugs", bugs, _RED if bugs > 3 else BLUE),
    ]
    for i, (label, val, color) in enumerate(kpi_cards):
        ac = color or BLUE
        _kpi_metric_card(
            reqs, f"{sid}_spk{i}", sid, right_x, right_y, right_w, _ENG_SP_KPI_H,
            label, str(val), accent=ac, value_pt=22,
        )
        right_y += _ENG_SP_KPI_H + _ENG_SP_KPI_GAP

    # ── INSIGHT BULLETS ──
    insights = (eng.get("insights") or {}).get("support_pressure", [])
    if insights:
        bullet_y = BODY_BOTTOM - (len(insights) * 22) - 4
        _eng_insight_bullets(reqs, sid, insights, MARGIN, bullet_y, CONTENT_W)

    return idx + 1


_PROJECT_SLIDE_SUBTITLE = {
    "HELP": "Support",
    "CUSTOMER": "Implementation escalations",
    "LEAN": "Engineering escalations",
}


def _eng_jira_project_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """Per-project Jira snapshot with status and assignee bar charts."""
    eng = report.get("eng_portfolio") or {}
    entry = report.get("_current_slide") or {}
    pk = (entry.get("jira_project") or "HELP").strip().upper()
    snapshots = eng.get("project_snapshots") or {}
    snap = snapshots.get(pk) or {}

    if snap.get("error") and "open_count" not in snap:
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"Jira project data ({pk}): {snap.get('error', 'unavailable')}",
        )

    title = entry.get("title") or f"{pk} — {_PROJECT_SLIDE_SUBTITLE.get(pk, pk)}"
    open_n = int(snap.get("open_count") or 0)
    by_status = snap.get("by_status_open") or {}
    median_open = snap.get("median_open_age_days")
    avg_cycle = snap.get("avg_resolved_cycle_days")
    res_6m = int(snap.get("resolved_in_6mo_count") or 0)
    assignee_rows = snap.get("assignee_resolved_table") or []

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    open_lbl = (
        f"Median age of open tickets: {median_open} d"
        if median_open is not None else
        "Median age of open tickets: —"
    )
    cycle_lbl = (
        f"Avg open→resolved (6 mo): {avg_cycle} d" if avg_cycle is not None else "Avg open→resolved (6 mo): —"
    )
    meta = (
        f"Total open: {open_n}   ·   {open_lbl}   ·   {cycle_lbl}   ·   Resolved (6 mo): {res_6m}"
    )
    _box(reqs, f"{sid}_meta", sid, MARGIN, BODY_Y, CONTENT_W, 30, meta)
    _style(reqs, f"{sid}_meta", 0, len(meta), size=10, color=GRAY, font=FONT)

    body_top = BODY_Y + 32
    col_gap = 24
    left_w = (CONTENT_W - col_gap) // 2
    right_w = CONTENT_W - left_w - col_gap
    left_x = MARGIN
    right_x = MARGIN + left_w + col_gap
    charts = report.get("_charts")

    # ── LEFT: open tickets by status (vertical bar chart) ──
    ly = body_top
    hist_h = "Open tickets by status"
    _box(reqs, f"{sid}_hh", sid, left_x, ly, left_w, 14, hist_h)
    _style(reqs, f"{sid}_hh", 0, len(hist_h), bold=True, size=10, color=NAVY, font=FONT)
    ly += 22

    stat_items = list(by_status.items())[:8]
    if stat_items and charts:
        try:
            from .charts import embed_chart
            ss_id, chart_id = charts.add_bar_chart(
                title=f"{pk} Open Tickets by Status",
                labels=[s for s, _ in stat_items],
                series={"Open tickets": [c for _, c in stat_items]},
                horizontal=False,
            )
            embed_chart(
                reqs, f"{sid}_status_chart", sid, ss_id, chart_id,
                left_x, ly, left_w, 188, linked=False,
            )
        except Exception as e:
            logger.warning("Jira project status chart failed (%s): %s", pk, e)

    if not stat_items:
        _box(reqs, f"{sid}_no_st", sid, left_x, ly + 68, left_w, 14, "No open tickets")
        _style(reqs, f"{sid}_no_st", 0, 14, size=9, color=GRAY, font=FONT)

    # ── RIGHT: top assignees by resolved volume (horizontal bar chart) ──
    ry = body_top
    bar_t = "Resolved tickets by assignee (6 mo)"
    _box(reqs, f"{sid}_th", sid, right_x, ry, right_w, 14, bar_t)
    _style(reqs, f"{sid}_th", 0, len(bar_t), bold=True, size=10, color=NAVY, font=FONT)
    ry += 22

    if assignee_rows and charts:
        try:
            from .charts import embed_chart
            assignee_items = assignee_rows[:8]
            ss_id, chart_id = charts.add_bar_chart(
                title=f"{pk} Resolved Tickets by Assignee",
                labels=[(row.get("assignee") or "Unassigned")[:24] for row in assignee_items],
                series={"Resolved (6 mo)": [int(row.get("6m", 0)) for row in assignee_items]},
                horizontal=True,
            )
            embed_chart(
                reqs, f"{sid}_assignee_chart", sid, ss_id, chart_id,
                right_x, ry, right_w, 188, linked=False,
            )
        except Exception as e:
            logger.warning("Jira project assignee chart failed (%s): %s", pk, e)

    if not assignee_rows:
        _box(reqs, f"{sid}_no_as", sid, right_x, ry + 56, right_w, 14, "No resolved tickets in last 6 months")
        _style(reqs, f"{sid}_no_as", 0, 36, size=8, color=GRAY, font=FONT)

    note = "Assignee chart shows resolved tickets in the last 6 months."
    _box(reqs, f"{sid}_fn", sid, MARGIN, BODY_BOTTOM - 12, CONTENT_W, 10, note)
    _style(reqs, f"{sid}_fn", 0, len(note), size=6, color=GRAY, font=FONT)

    return idx + 1


def _eng_help_volume_trends_slide(reqs: list, sid: str, report: dict, idx: int) -> int:
    """HELP monthly created vs resolved trends for all, escalated, and non-escalated tickets."""
    eng = report.get("eng_portfolio") or {}
    raw_trends = eng.get("help_ticket_trends")

    if raw_trends is None:
        try:
            from .jira_client import get_shared_jira_client
            raw_trends = get_shared_jira_client()._get_help_ticket_volume_trends()
            eng["help_ticket_trends"] = raw_trends
            report.setdefault("eng_portfolio", eng)
            logger.debug("eng_help_volume_trends: fetched HELP trends on demand (no eng_portfolio)")
        except Exception as e:
            logger.warning("eng_help_volume_trends: on-demand HELP trends fetch failed: %s", e)
            raw_trends = {"error": str(e)}

    trends = raw_trends if isinstance(raw_trends, dict) else {}
    err = trends.get("error")
    all_months = list(trends.get("all") or [])
    escalated_months = list(trends.get("escalated") or [])
    non_escalated_months = list(trends.get("non_escalated") or [])
    charts = report.get("_charts")

    if err:
        return _missing_data_slide(
            reqs, sid, report, idx,
            f"HELP ticket volume trends — Jira error: {err}",
        )
    if not all_months:
        return _missing_data_slide(
            reqs, sid, report, idx,
            "HELP ticket volume trends — no monthly series (unexpected empty response)",
        )
    if not charts:
        return _missing_data_slide(
            reqs, sid, report, idx,
            "HELP ticket volume trends — chart embedding unavailable",
        )

    recent = all_months[-3:]
    recent_created = sum(m.get("created", 0) for m in recent)
    recent_resolved = sum(m.get("resolved", 0) for m in recent)
    net = recent_created - recent_resolved
    if net > 10:
        title = f"HELP Volume Rising — {net} More Tickets Created Than Resolved in Last 3 Months"
    elif net < -10:
        title = f"HELP Backlog Pressure Easing — {abs(net)} More Tickets Resolved Than Created in Last 3 Months"
    else:
        title = "HELP Ticket Volume Trends — Created vs Resolved"

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, title)

    ctx = "Last 12 months   ·   Monthly created vs monthly resolved   ·   Split into all, escalated, and non-escalated"
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y, CONTENT_W, 16, ctx)
    _style(reqs, f"{sid}_ctx", 0, len(ctx), size=9, color=GRAY, font=FONT)

    legend_y = BODY_Y + 18
    _rect(reqs, f"{sid}_lg_created", sid, MARGIN, legend_y + 4, 18, 3, NAVY)
    _box(reqs, f"{sid}_lg_created_t", sid, MARGIN + 24, legend_y, 48, 12, "Created")
    _style(reqs, f"{sid}_lg_created_t", 0, 7, bold=True, size=9, color=NAVY, font=FONT)
    created_resolved = {"red": 0.90, "green": 0.40, "blue": 0.00}
    _rect(reqs, f"{sid}_lg_resolved", sid, MARGIN + 76, legend_y + 4, 18, 3, created_resolved)
    _box(reqs, f"{sid}_lg_resolved_t", sid, MARGIN + 100, legend_y, 54, 12, "Resolved")
    _style(reqs, f"{sid}_lg_resolved_t", 0, 8, bold=True, size=9, color=NAVY, font=FONT)

    from .charts import embed_chart

    top_y = BODY_Y + 34
    top_gap = 16
    top_chart_w = (CONTENT_W - top_gap) // 2
    top_chart_h = 82
    left_x = MARGIN
    right_x = MARGIN + top_chart_w + top_gap

    _box(reqs, f"{sid}_all_h", sid, left_x, top_y, top_chart_w, 14, "All HELP tickets")
    _style(reqs, f"{sid}_all_h", 0, 16, bold=True, size=10, color=NAVY, font=FONT)
    top_chart_y = top_y + 18
    ss_id, chart_id = charts.add_line_chart(
        title="",
        labels=[m.get("label", "") for m in all_months],
        series={
            "Created": [m.get("created", 0) for m in all_months],
            "Resolved": [m.get("resolved", 0) for m in all_months],
        },
        series_colors=[NAVY, created_resolved],
        show_legend=False,
        axis_font_size=9,
        line_width=3,
    )
    embed_chart(reqs, f"{sid}_all_chart", sid, ss_id, chart_id, left_x, top_chart_y, top_chart_w, top_chart_h, linked=False)

    _box(reqs, f"{sid}_esc_h", sid, right_x, top_y, top_chart_w, 14, "HELP tickets with jira_escalated label")
    _style(reqs, f"{sid}_esc_h", 0, 38, bold=True, size=10, color=NAVY, font=FONT)
    esc_chart_y = top_y + 18
    ss_id2, chart_id2 = charts.add_line_chart(
        title="",
        labels=[m.get("label", "") for m in escalated_months],
        series={
            "Created": [m.get("created", 0) for m in escalated_months],
            "Resolved": [m.get("resolved", 0) for m in escalated_months],
        },
        series_colors=[NAVY, created_resolved],
        show_legend=False,
        axis_font_size=9,
        line_width=3,
    )
    embed_chart(reqs, f"{sid}_esc_chart", sid, ss_id2, chart_id2, right_x, esc_chart_y, top_chart_w, top_chart_h, linked=False)

    bottom_chart_w = 436
    bottom_chart_h = 82
    bottom_x = MARGIN + (CONTENT_W - bottom_chart_w) / 2
    bottom_y = top_chart_y + top_chart_h + 18
    _box(reqs, f"{sid}_non_h", sid, bottom_x, bottom_y, bottom_chart_w, 14, "HELP tickets excluding jira_escalated")
    _style(reqs, f"{sid}_non_h", 0, 37, bold=True, size=10, color=NAVY, font=FONT)
    non_chart_y = bottom_y + 18
    ss_id3, chart_id3 = charts.add_line_chart(
        title="",
        labels=[m.get("label", "") for m in non_escalated_months],
        series={
            "Created": [m.get("created", 0) for m in non_escalated_months],
            "Resolved": [m.get("resolved", 0) for m in non_escalated_months],
        },
        series_colors=[NAVY, created_resolved],
        show_legend=False,
        axis_font_size=9,
        line_width=3,
    )
    embed_chart(reqs, f"{sid}_non_chart", sid, ss_id3, chart_id3, bottom_x, non_chart_y, bottom_chart_w, bottom_chart_h, linked=False)

    return idx + 1


def _sf_format_cell(val: Any, max_len: int = 44) -> str:
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        s = json.dumps(val, default=str)
    else:
        s = str(val)
    if len(s) > max_len:
        return s[: max_len - 1] + "…"
    return s


def _sf_records_to_table(
    records: list[dict[str, Any]],
    *,
    max_cols: int = 7,
    max_rows: int = 12,
) -> tuple[list[str], list[list[str]]]:
    if not records:
        return [], []
    keys: list[str] = []
    for rec in records[:40]:
        for k in rec.keys():
            if k not in keys:
                keys.append(k)
    keys = keys[:max_cols]
    rows = [[_sf_format_cell(rec.get(k)) for k in keys] for rec in records[:max_rows]]
    return keys, rows


def _sf_category_records(sfc: dict[str, Any], category: str) -> list[dict[str, Any]]:
    """Resolve records for a deck ``sf_category`` (must match ``_salesforce_category_slide``)."""
    cat = (category or "").strip()
    if cat == "entity_accounts":
        return list(sfc.get("accounts") or [])
    return list((sfc.get("categories") or {}).get(cat) or [])


def _filter_salesforce_comprehensive_slide_plan(
    slide_plan: list[dict[str, Any]],
    sfc: dict[str, Any],
) -> list[dict[str, Any]]:
    """Drop ``salesforce_category`` entries with no rows (cover, data_quality, etc. stay)."""
    out: list[dict[str, Any]] = []
    for entry in slide_plan:
        st = entry.get("slide_type", entry.get("id", ""))
        if st != "salesforce_category":
            out.append(entry)
            continue
        if _sf_category_records(sfc, entry.get("sf_category") or ""):
            out.append(entry)
    return out


def _salesforce_comprehensive_cover_slide(reqs, sid, report, idx):
    """Intro slide for the Salesforce comprehensive deck."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Salesforce — comprehensive export")
    sfc = report.get("salesforce_comprehensive") or {}
    customer = report.get("customer", "")
    parts: list[str] = []
    err = sfc.get("error")
    if err:
        parts.append(f"Setup: {err}")
    if not sfc.get("matched"):
        parts.append(f'No Customer Entity account matched for "{customer}".')
    else:
        n_acc = len(sfc.get("accounts") or [])
        parts.append(f"Customer: {customer}")
        parts.append(f"Matched {n_acc} Customer Entity account(s).")
        exp = sfc.get("account_ids_expanded") or sfc.get("account_ids") or []
        if len(exp) > n_acc:
            parts.append(
                f"Queries include {len(exp)} account Id(s) (entity row(s) plus child accounts via ParentId)."
            )
        elif exp:
            parts.append(f"Queries scoped to {len(exp)} account Id(s).")
        rl = sfc.get("row_limit", 75)
        parts.append(
            f"Each related object is capped at ~{rl} rows (API first page); not a full data warehouse export."
        )
    parts.append("Products and price books are org-wide samples (not filtered to this account).")
    body = "\n".join(parts)
    oid = f"{sid}_body"
    body_h = max(80.0, BODY_BOTTOM - BODY_Y - 8)
    _wrap_box(reqs, oid, sid, MARGIN, BODY_Y, CONTENT_W, body_h, body)
    _style(reqs, oid, 0, len(body), size=11, color=NAVY, font=FONT)
    return idx + 1


def _salesforce_category_slide(reqs, sid, report, idx):
    """One table per mainstream Salesforce category (see deck ``sf_category``)."""
    entry = report.get("_current_slide") or {}
    cat = (entry.get("sf_category") or "").strip()
    title = (entry.get("title") or cat.replace("_", " ").title())[:100]
    sfc = report.get("salesforce_comprehensive") or {}

    if "salesforce_comprehensive" not in report:
        return _missing_data_slide(reqs, sid, report, idx, "salesforce_comprehensive payload")

    if not cat:
        return _missing_data_slide(reqs, sid, report, idx, "sf_category not set on slide")

    records = _sf_category_records(sfc, cat)

    err_note = (sfc.get("category_errors") or {}).get(cat)

    # Slides grows table rows when 9pt text wraps in narrow cells; 12pt nominal height underruns badly.
    row_h = 28.0
    y0 = BODY_Y + (38 if err_note else 0)
    bottom_pad = 10.0
    avail_h = BODY_BOTTOM - y0 - bottom_pad
    # Table is 1 header row + N data rows, each row_h pt tall in API layout.
    rows_per_page = max(2, int(avail_h // row_h) - 1)

    if not records:
        _slide(reqs, sid, idx)
        _bg(reqs, sid, LIGHT)
        _slide_title(reqs, sid, title)
        y = BODY_Y
        if err_note:
            banner = f"Query error: {err_note[:140]}"
            bid = f"{sid}_warn"
            _box(reqs, bid, sid, MARGIN, y, CONTENT_W, 32, banner)
            _style(reqs, bid, 0, len(banner), size=8, color=GRAY, font=FONT)
            y += 38
        msg = "No records for this category." + (" (see query error above)" if err_note else "")
        eid = f"{sid}_empty"
        _box(reqs, eid, sid, MARGIN, y, CONTENT_W, 36, msg)
        _style(reqs, eid, 0, len(msg), size=11, color=NAVY, font=FONT)
        return idx + 1

    chunks = _cap_chunk_list(
        [records[i : i + rows_per_page] for i in range(0, len(records), rows_per_page)]
    )
    oids: list[str] = []
    for pi, chunk in enumerate(chunks):
        page_sid = f"{sid}_p{pi}" if len(chunks) > 1 else sid
        oids.append(page_sid)
        _slide(reqs, page_sid, idx + pi)
        _bg(reqs, page_sid, LIGHT)
        page_title = title if len(chunks) == 1 else f"{title} ({pi + 1} of {len(chunks)})"
        _slide_title(reqs, page_sid, page_title)
        y = BODY_Y
        if pi == 0 and err_note:
            banner = f"Query error: {err_note[:140]}"
            bid = f"{page_sid}_warn"
            _box(reqs, bid, page_sid, MARGIN, y, CONTENT_W, 32, banner)
            _style(reqs, bid, 0, len(banner), size=8, color=GRAY, font=FONT)
            y += 38
        headers, rows = _sf_records_to_table(chunk, max_rows=len(chunk))
        if not headers:
            continue
        n = len(headers)
        col_w = min(118.0, CONTENT_W / max(1, n))
        col_widths = [col_w] * n
        _simple_table(reqs, f"{page_sid}_tbl", page_sid, MARGIN, y, col_widths, row_h, headers, rows)
    return idx + len(chunks), oids


# ── Composable API (agent builds deck slide by slide) ──

# Maps slide type names to builder functions and the report keys they require
_SLIDE_BUILDERS = {
    "title": _title_slide,
    "health": _health_slide,
    "engagement": _engagement_slide,
    "sites": _sites_slide,
    "features": _features_slide,
    "champions": _champions_slide,
    "benchmarks": _benchmarks_slide,
    "exports": _exports_slide,
    "depth": _depth_slide,
    "kei": _kei_slide,
    "guides": _guides_slide,
    "jira": _jira_slide,
    "customer_ticket_metrics": _customer_ticket_metrics_slide,
    "support_recent_opened": _support_recent_opened_slide,
    "support_recent_closed": _support_recent_closed_slide,
    "custom": _custom_slide,
    "signals": _signals_slide,
    "platform_health": _platform_health_slide,
    "supply_chain": _supply_chain_slide,
    "platform_value": _platform_value_slide,
    "data_quality": _data_quality_slide,
    "portfolio_title": _portfolio_title_slide,
    "portfolio_signals": _portfolio_signals_slide,
    "portfolio_trends": _portfolio_trends_slide,
    "portfolio_leaders": _portfolio_leaders_slide,
    "team": _team_slide,
    "sla_health": _sla_health_slide,
    "cross_validation": _cross_validation_slide,
    "engineering": _engineering_slide,
    "enhancements": _enhancement_requests_slide,
    "support_breakdown": _support_breakdown_slide,
    "qbr_cover": _qbr_cover_slide,
    "qbr_agenda": _qbr_agenda_slide,
    "qbr_divider": _qbr_divider_slide,
    "qbr_deployment": _qbr_deployment_slide,
    "eng_portfolio_title": _eng_portfolio_title_slide,
    "eng_sprint_snapshot": _eng_sprint_snapshot_slide,
    "eng_bug_health": _eng_bug_health_slide,
    "eng_velocity": _eng_velocity_slide,
    "eng_enhancements": _eng_enhancements_open_slide,
    "eng_enhancements_shipped": _eng_enhancements_shipped_slide,
    "eng_support_pressure": _eng_support_pressure_slide,
    "eng_jira_project": _eng_jira_project_slide,
    "eng_help_volume_trends": _eng_help_volume_trends_slide,
    "salesforce_comprehensive_cover": _salesforce_comprehensive_cover_slide,
    "salesforce_category": _salesforce_category_slide,
    "cohort_deck_title": _cohort_deck_title_slide,
    "cohort_summary": _cohort_summary_slide,
    "cohort_profiles": _cohort_profiles_slide,
    "cohort_findings": _cohort_findings_slide,
}

# Which report keys each slide type needs (so the agent knows what data to supply)
SLIDE_DATA_REQUIREMENTS = {
    "title": ["customer", "days", "generated", "account"],
    "health": ["engagement", "benchmarks", "account"],
    "engagement": ["engagement", "account"],
    "sites": ["sites"],
    "features": ["top_pages", "top_features"],
    "champions": ["champions", "at_risk_users"],
    "benchmarks": ["benchmarks", "account"],
    "exports": ["exports"],
    "depth": ["depth"],
    "kei": ["kei"],
    "guides": ["guides"],
    "jira": ["jira"],
    "customer_ticket_metrics": ["jira"],
    "support_recent_opened": ["jira"],
    "support_recent_closed": ["jira"],
    "custom": ["title", "sections"],
    "signals": ["signals"],
    "platform_health": ["csr"],
    "supply_chain": ["csr"],
    "platform_value": ["csr"],
    "sla_health": ["jira"],
    "cross_validation": ["csr", "sites", "engagement"],
    "engineering": ["jira"],
    "enhancements": ["jira"],
    "support_breakdown": ["jira"],
    "data_quality": [],
    "portfolio_title": ["customer_count", "days", "generated"],
    "portfolio_signals": ["portfolio_signals"],
    "portfolio_trends": ["portfolio_trends"],
    "portfolio_leaders": ["portfolio_leaders"],
    "cohort_deck_title": ["customer_count", "days", "generated"],
    "cohort_summary": ["cohort_digest"],
    "cohort_profiles": ["cohort_digest"],
    "cohort_findings": ["cohort_findings_bullets"],
    "team": ["customer"],
    "qbr_cover": ["customer", "days"],
    "qbr_agenda": [],
    "qbr_divider": [],
    "qbr_deployment": ["sites"],
    "eng_portfolio_title": ["eng_portfolio"],
    "eng_sprint_snapshot": ["eng_portfolio"],
    "eng_bug_health": ["eng_portfolio"],
    "eng_velocity": ["eng_portfolio"],
    "eng_enhancements": ["eng_portfolio"],
    "eng_enhancements_shipped": ["eng_portfolio"],
    "eng_support_pressure": ["eng_portfolio"],
    "eng_jira_project": ["eng_portfolio"],
    "eng_help_volume_trends": ["eng_portfolio"],
    "salesforce_comprehensive_cover": ["salesforce_comprehensive"],
    "salesforce_category": ["salesforce_comprehensive"],
}


_output_folder_cache: tuple[str, str] | None = None  # (date_str, folder_id)


def _get_deck_output_folder() -> str | None:
    """Return the ID of today's date-stamped subfolder (e.g. Decks-2026-03-06), creating it if needed."""
    global _output_folder_cache
    if not GOOGLE_DRIVE_FOLDER_ID:
        return None
    today = datetime.date.today().isoformat()
    if _output_folder_cache and _output_folder_cache[0] == today:
        return _output_folder_cache[1]
    from .drive_config import _find_or_create_folder
    folder_id = _find_or_create_folder(f"Decks-{today}", GOOGLE_DRIVE_FOLDER_ID)
    _output_folder_cache = (today, folder_id)
    return folder_id


def create_empty_deck(customer: str, days: int = 30, deck_name: str | None = None) -> dict[str, Any]:
    """Create an empty presentation. Returns {deck_id, url} for use with add_slide."""
    try:
        slides_service, drive_service, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    label = deck_name or "Usage Health Review"
    title = f"{customer} — {label} ({_date_range(days)})"
    try:
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            file_meta["parents"] = [output_folder]
        f = drive_service.files().create(body=file_meta).execute()
        deck_id = f["id"]
        logger.info("Created deck %s: %s", deck_id, title)
    except HttpError as e:
        return {"error": str(e)}

    # Delete the default blank slide
    try:
        pres = slides_service.presentations().get(presentationId=deck_id).execute()
        default_id = pres["slides"][0]["objectId"]
        slides_presentations_batch_update(
            slides_service,
            deck_id,
            [{"deleteObject": {"objectId": default_id}}],
        )
    except Exception:
        pass

    return {
        "deck_id": deck_id,
        "url": f"https://docs.google.com/presentation/d/{deck_id}/edit",
    }


_slide_counter: dict[str, int] = {}


def add_slide(deck_id: str, slide_type: str, data: dict[str, Any]) -> dict[str, Any]:
    """Add one slide to an existing deck.

    Args:
        deck_id: Presentation ID from create_empty_deck.
        slide_type: One of: title, health, engagement, sites, features, champions, benchmarks, exports, depth, kei, guides, custom, signals.
        data: Dict with the keys required for that slide type (see SLIDE_DATA_REQUIREMENTS).

    Returns:
        {slide_type, status} or {error}.
    """
    builder = _SLIDE_BUILDERS.get(slide_type)
    if not builder:
        return {"error": f"Unknown slide type '{slide_type}'. Valid: {', '.join(_SLIDE_BUILDERS)}"}

    try:
        slides_service, _ds, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    # Use local counter as insertion index to avoid an API round-trip per slide
    count = _slide_counter.get(deck_id, 0)
    _slide_counter[deck_id] = count + 1
    idx = count
    sid = f"s_{slide_type}_{count}"

    reqs: list[dict] = []
    try:
        ret = builder(reqs, sid, data, idx)
        new_idx, note_ids = _normalize_builder_return(ret, sid)
    except (KeyError, TypeError, IndexError) as e:
        required = SLIDE_DATA_REQUIREMENTS.get(slide_type, [])
        return {
            "error": f"Slide '{slide_type}' data is missing required key: {e}. Required keys: {required}",
            "slide_type": slide_type,
        }

    if not reqs:
        return {"slide_type": slide_type, "status": "skipped (no data)"}

    try:
        presentations_batch_update_chunked(slides_service, deck_id, reqs)
    except HttpError as e:
        return {"error": str(e), "slide_type": slide_type}

    note_entry = {
        "id": slide_type,
        "slide_type": slide_type,
        "title": data.get("title", slide_type.replace("_", " ").title()),
    }
    note_payload = dict(data)
    note_payload["_current_slide"] = note_entry
    notes = _build_slide_jql_speaker_notes(note_payload, note_entry)
    if note_ids:
        n = set_speaker_notes_batch(slides_service, deck_id, [(nid, notes) for nid in note_ids])
        if n < len(note_ids):
            logger.warning("Could not write JQL speaker notes for %d/%d slides in deck %s", len(note_ids) - n, len(note_ids), deck_id[:12])

    return {"slide_type": slide_type, "status": "added", "position": idx + 1, "pages": len(note_ids)}


# ── Monolith deck creation (deck-definition-driven) ──

def create_health_deck(
    report: dict[str, Any],
    deck_id: str = "cs_health_review",
    thumbnails: bool = True,
    output_folder_id: str | None = None,
) -> dict[str, Any]:
    """Create a deck from a customer health report using a deck definition.

    Args:
        report: Full customer health report from PendoClient.get_customer_health_report().
        deck_id: Which deck definition to use. Defaults to 'cs_health_review'.
        thumbnails: Whether to export slide thumbnails. Disable for batch runs.
        output_folder_id: Optional Drive folder id for the new presentation. When omitted,
            uses today's ``Decks-{date}`` folder under ``GOOGLE_DRIVE_FOLDER_ID`` (if set).
    """
    if "error" in report:
        return {"error": report["error"]}

    is_portfolio = report.get("type") == "portfolio"
    customer = report.get("customer", "Portfolio") if not is_portfolio else "Portfolio"
    days = report.get("days", 30)
    quarter_label = report.get("quarter")

    from .qa import qa
    qa.begin(customer)

    try:
        slides_service, drive_service, sheets_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    # Make services accessible to slide builders via the report dict
    report["_slides_svc"] = slides_service
    report["_drive_svc"] = drive_service

    from .deck_loader import resolve_deck

    resolved = resolve_deck(deck_id, customer)
    if resolved.get("error"):
        return {"error": resolved["error"]}

    deck_name = resolved.get("name", "Health Review")
    date_str = _date_range(days, quarter_label, report.get("quarter_start"), report.get("quarter_end"))
    if is_portfolio:
        title = f"{deck_name} ({date_str})"
    else:
        title = f"{customer} — {deck_name} ({date_str})"

    slide_plan: list[dict[str, Any]] = list(resolved.get("slides") or [])

    if deck_id == "salesforce_comprehensive":
        from .data_source_health import _salesforce_configured

        empty_sf = {
            "customer": customer,
            "accounts": [],
            "account_ids": [],
            "matched": False,
            "opportunity_count_this_year": 0,
            "pipeline_arr": 0.0,
            "row_limit": 75,
            "categories": {},
            "category_errors": {},
        }
        if _salesforce_configured():
            try:
                from .salesforce_client import SalesforceClient

                report["salesforce_comprehensive"] = SalesforceClient().get_customer_salesforce_comprehensive(
                    customer
                )
            except Exception as e:
                logger.warning("Salesforce comprehensive fetch failed: %s", e)
                report["salesforce_comprehensive"] = {
                    **empty_sf,
                    "error": str(e)[:500],
                }
        else:
            report["salesforce_comprehensive"] = {**empty_sf, "error": "Salesforce not configured"}

        slide_plan = _filter_salesforce_comprehensive_slide_plan(
            slide_plan, report.get("salesforce_comprehensive") or {}
        )

    if not slide_plan:
        logger.error(
            "create_health_deck: empty slide plan (deck_id=%s customer=%r). "
            "Check decks/*.yaml vs slides/, Drive bpo-config sync, and per-customer slide filters.",
            deck_id,
            customer,
        )
        return {
            "error": "Deck has no slides to generate (resolved plan is empty).",
            "hint": "Verify deck YAML slide IDs exist in slides/. If using Drive config, ensure "
            "bpo-config/decks and slides match the repo. Slides with customers: [...] exclude "
            "everyone except listed customers.",
            "customer": customer,
            "deck_id": deck_id,
        }

    try:
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = output_folder_id if output_folder_id else _get_deck_output_folder()
        if output_folder:
            file_meta["parents"] = [output_folder]
        file = drive_service.files().create(body=file_meta).execute()
        pres_id = file["id"]
        logger.info("Created presentation %s: %s", pres_id, title)
    except HttpError as e:
        err_str = str(e)
        if "rate" in err_str.lower() or "quota" in err_str.lower():
            return {"error": f"Rate limit: {err_str}. Wait and retry."}
        return {"error": err_str}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            return {"error": str(e), "hint": hint, "customer": customer, "deck_id": deck_id}
        raise

    # Provide a DeckCharts instance for Slides embeds backed by Google Sheets.
    from .charts import DeckCharts
    report["_charts"] = DeckCharts(title)

    report["_slide_plan"] = slide_plan
    reqs: list[dict] = []
    idx = 1
    note_targets: list[tuple[str, dict[str, Any]]] = []

    for entry in slide_plan:
        slide_type = entry.get("slide_type", entry["id"])
        builder = _SLIDE_BUILDERS.get(slide_type)
        if not builder:
            logger.warning(
                "create_health_deck: no _SLIDE_BUILDERS entry for slide_type=%r (deck %s entry id=%r)",
                slide_type,
                deck_id,
                entry.get("id"),
            )
            continue
        report["_current_slide"] = entry
        sid = f"s_{entry['id']}_{idx}"
        ret = builder(reqs, sid, report, idx)
        next_idx, note_ids = _normalize_builder_return(ret, sid)
        if slide_type == "cohort_profiles" and note_ids:
            blks = report.get("_cohort_profile_speaker_note_blocks") or []
            for i, nid in enumerate(note_ids):
                note_entry = dict(entry)
                if i < len(blks):
                    note_entry["_cohort_profile_block"] = blks[i]
                note_targets.append((nid, note_entry))
        else:
            for nid in note_ids:
                note_targets.append((nid, dict(entry)))
        idx = next_idx

    slides_created = idx - 1

    try:
        pres = slides_service.presentations().get(presentationId=pres_id).execute()
        default_id = pres["slides"][0]["objectId"]
        if slides_created > 0:
            reqs.append({"deleteObject": {"objectId": default_id}})
        else:
            logger.error(
                "create_health_deck: built 0 slides (deck_id=%s customer=%r plan_len=%d). "
                "Leaving default slide; check warnings above for missing builders.",
                deck_id,
                customer,
                len(slide_plan),
            )
    except Exception:
        pass

    try:
        presentations_batch_update_chunked(slides_service, pres_id, reqs)
    except HttpError as e:
        logger.exception("Failed to build slides")
        return {"error": str(e), "presentation_id": pres_id}
    except Exception as e:
        hint = _google_api_unreachable_hint(e)
        if hint:
            return {"error": str(e), "hint": hint, "presentation_id": pres_id, "customer": customer, "deck_id": deck_id}
        raise

    if slides_created == 0:
        url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
        return {
            "error": "No slides were built — every slide_type may be unknown or builders returned nothing.",
            "hint": "See logs for slide_type warnings. Compare slides/*.yaml slide_type to src/slides_client.py _SLIDE_BUILDERS.",
            "presentation_id": pres_id,
            "url": url,
            "customer": customer,
            "slides_created": 0,
        }

    notes_items = [(sid, _build_slide_jql_speaker_notes(report, entry)) for sid, entry in note_targets]
    if notes_items:
        n = set_speaker_notes_batch(slides_service, pres_id, notes_items)
        logger.info("Speaker notes: wrote %d/%d slide notes in single batchUpdate", n, len(notes_items))

    result = {
        "presentation_id": pres_id,
        "url": f"https://docs.google.com/presentation/d/{pres_id}/edit",
        "customer": customer,
        "slides_created": slides_created,
    }

    if thumbnails:
        try:
            thumbs = export_slide_thumbnails(pres_id)
            result["thumbnails"] = [str(p) for p in thumbs]
            logger.info("Saved %d slide thumbnails for %s", len(thumbs), customer)
        except Exception as e:
            logger.warning("Thumbnail export failed: %s", e)

    return result


def create_portfolio_deck(
    days: int = 30,
    max_customers: int | None = None,
    quarter: "QuarterRange | None" = None,
) -> dict[str, Any]:
    """Generate a single portfolio-level deck across all customers."""
    from .pendo_portfolio_snapshot_drive import try_load_portfolio_snapshot_for_request

    report = try_load_portfolio_snapshot_for_request(days, max_customers)
    if report is None:
        from .pendo_client import PendoClient

        client = PendoClient()
        report = client.get_portfolio_report(days=days, max_customers=max_customers)
    if quarter:
        report["quarter"] = quarter.label
        report["quarter_start"] = quarter.start.isoformat()
        report["quarter_end"] = quarter.end.isoformat()
    return create_health_deck(report, deck_id="portfolio_review")


def create_cohort_deck(
    days: int = 30,
    max_customers: int | None = None,
    quarter: "QuarterRange | None" = None,
    thumbnails: bool = False,
    output_folder_id: str | None = None,
    portfolio_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Single deck: cohort buckets from cohorts.yaml + portfolio metrics (max 10 profile slides).

    If *portfolio_report* is supplied the expensive Pendo preload + customer
    iteration is skipped entirely — the caller already computed it.

    Otherwise, when the resolved snapshot folder (``GOOGLE_QBR_GENERATOR_FOLDER_ID`` /
    ``Cache`` (QBR generator subfolder) or ``BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID``) has a fresh JSON
    file (see ``pendo_portfolio_snapshot_drive``), it is used instead of calling Pendo.
    """
    if portfolio_report is not None:
        report = portfolio_report
    else:
        from .pendo_portfolio_snapshot_drive import try_load_portfolio_snapshot_for_request

        report = try_load_portfolio_snapshot_for_request(days, max_customers)
        if report is None:
            from .pendo_client import PendoClient

            client = PendoClient()
            report = client.get_portfolio_report(days=days, max_customers=max_customers)

    if quarter:
        report["quarter"] = quarter.label
        report["quarter_start"] = quarter.start.isoformat()
        report["quarter_end"] = quarter.end.isoformat()
    logger.info(
        "cohort_review: portfolio report ready (%d customers) — sending to Google Slides",
        report.get("customer_count", 0),
    )

    try:
        from .data_source_health import _salesforce_configured
        if _salesforce_configured():
            from .salesforce_client import SalesforceClient
            sf = SalesforceClient()
            digest = report.get("cohort_digest") or {}
            all_names: list[str] = []
            for block in digest.values():
                if isinstance(block, dict):
                    all_names.extend(block.get("customers") or [])
            if all_names:
                arr_map = sf.get_arr_by_customer_names(all_names)
                report["_arr_by_customer"] = arr_map
                logger.info("cohort_review: loaded ARR for %d/%d customers from Salesforce",
                            len(arr_map), len(all_names))

                active_names = sf.get_active_customer_names(all_names)
                churned = set(all_names) - active_names
                if churned:
                    logger.info("cohort_review: filtering %d churned customer(s) from cohort slides", len(churned))
                    from .pendo_client import compute_cohort_portfolio_rollup
                    customers = report.get("customers") or []
                    active_summaries = [s for s in customers if s.get("customer") not in churned]
                    new_digest, new_findings = compute_cohort_portfolio_rollup(active_summaries)
                    report["cohort_digest"] = new_digest
                    report["cohort_findings_bullets"] = new_findings
                    report["customer_count"] = len(active_summaries)
                    report["_churned_customers"] = sorted(churned)
    except Exception as e:
        logger.warning("cohort_review: Salesforce ARR lookup failed (continuing without): %s", e)

    return create_health_deck(
        report,
        deck_id="cohort_review",
        thumbnails=thumbnails,
        output_folder_id=output_folder_id,
    )


def create_health_decks_for_customers(
    customer_names: list[str],
    days: int = 30,
    max_customers: int | None = None,
    deck_id: str = "cs_health_review",
    workers: int = 4,
    thumbnails: bool = False,
    quarter: "QuarterRange | None" = None,
) -> list[dict[str, Any]]:
    """Create one deck per customer using a deck definition (parallel).

    Args:
        customer_names: List of customer names to generate decks for.
        days: Lookback window in days.
        max_customers: Cap on how many to generate.
        deck_id: Which deck definition to use (default: cs_health_review).
        workers: Concurrent deck-creation threads (default 4).
        thumbnails: Export slide thumbnails (default False for batch — saves API quota).
        quarter: Optional QuarterRange to label slides with quarter info.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .pendo_client import PendoClient

    client = PendoClient()
    client.preload(days)
    customers = customer_names[:max_customers] if max_customers else customer_names
    quarter_label = quarter.label if quarter else None
    quarter_start = quarter.start.isoformat() if quarter else None
    quarter_end = quarter.end.isoformat() if quarter else None

    def _build_one(idx_name: tuple[int, str]) -> dict[str, Any]:
        i, name = idx_name
        logger.debug("Generating deck %d/%d: %s (%s)", i + 1, len(customers), name, deck_id)
        try:
            report = client.get_customer_health_report(name, days=days)
            if quarter_label:
                report["quarter"] = quarter_label
                report["quarter_start"] = quarter_start
                report["quarter_end"] = quarter_end
            return create_health_deck(report, deck_id=deck_id, thumbnails=thumbnails)
        except Exception as e:
            return {"error": str(e), "customer": name}

    results: list[dict[str, Any]] = [{}] * len(customers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_build_one, (i, n)): i for i, n in enumerate(customers)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = {"error": str(e), "customer": customers[idx]}
            r = results[idx]
            if "error" in r and "403" in str(r.get("error", "")):
                logger.error("Got 403 for %s — cancelling remaining.", customers[idx])
                for f in futures:
                    f.cancel()
                break

    return results


# ── Legacy (backward compat) ──

def create_deck_for_customer(customer, sites, days=30):
    if not sites:
        return {"error": f"No sites for '{customer}'"}
    try:
        slides_service, drive_service, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}
    title = f"{customer} - Usage Report ({_date_range(days)})"
    try:
        meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            meta["parents"] = [output_folder]
        f = drive_service.files().create(body=meta).execute()
        pid = f["id"]
    except HttpError as e:
        return {"error": str(e)}
    r = []
    ix = 1
    for i, s in enumerate(sites):
        sid = f"ls_{i}"
        r.append({"createSlide": {"objectId": sid, "insertionIndex": ix}}); ix += 1
        _box(r, f"lt_{i}", sid, 60, 40, 600, 50, s.get("sitename", "?"))
        body = f"Page views: {s.get('page_views',0)}\nFeature clicks: {s.get('feature_clicks',0)}\nEvents: {s.get('total_events',0)}\nMinutes: {s.get('total_minutes',0)}"
        _box(r, f"lb_{i}", sid, 60, 100, 600, 280, body)
    try:
        presentations_batch_update_chunked(slides_service, pid, r)
    except HttpError as e:
        return {"error": str(e), "presentation_id": pid}
    return {"presentation_id": pid, "url": f"https://docs.google.com/presentation/d/{pid}/edit", "customer": customer, "slides_created": len(sites)}


def create_decks_for_all_customers(by_customer, customer_list, days=30, delay_seconds=2.0, max_customers=None):
    cs = customer_list[:max_customers] if max_customers else customer_list
    results = []
    for i, c in enumerate(cs):
        if i > 0:
            time.sleep(delay_seconds)
        results.append(create_deck_for_customer(c, by_customer.get(c, []), days))
        if "error" in results[-1] and "403" in str(results[-1].get("error", "")):
            results.append({"error": "Stopped: 403.", "customers_attempted": i + 1}); break
    return results


# ── Slide thumbnail export ──

def export_slide_thumbnails(
    presentation_id: str,
    output_dir: str | Path | None = None,
    size: str = "LARGE",
) -> list[Path]:
    """Download PNG thumbnails for every slide in a presentation.

    Args:
        presentation_id: Google Slides presentation ID or full URL.
        output_dir: Where to save PNGs. Defaults to a temp directory.
        size: Thumbnail size — "SMALL" (default 200px) or "LARGE" (default 800px).

    Returns:
        List of saved PNG file paths.
    """
    import re
    import tempfile
    import urllib.request

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", presentation_id)
    pres_id = match.group(1) if match else presentation_id

    slides_service, _ds, _ = _get_service()
    pres = slides_service.presentations().get(presentationId=pres_id).execute()
    title = pres.get("title", pres_id)
    slides = pres.get("slides", [])

    if not slides:
        logger.warning("Presentation %s has no slides", pres_id)
        return []

    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix=f"bpo-thumbs-{pres_id[:12]}-"))
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []
    for i, slide in enumerate(slides):
        page_id = slide["objectId"]
        thumb = slides_service.presentations().pages().getThumbnail(
            presentationId=pres_id,
            pageObjectId=page_id,
            thumbnailProperties_thumbnailSize=size,
        ).execute()
        url = thumb["contentUrl"]
        dest = out / f"slide_{i + 1:02d}.png"
        urllib.request.urlretrieve(url, str(dest))
        saved.append(dest)

    logger.info("Exported %d thumbnails for '%s' → %s", len(saved), title, out)
    return saved
