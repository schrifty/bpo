#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# PURPOSE — CSM *time savings* (lookup / integration workload), not a full audit.
#
# Customer Success already knows trivial deck hygiene: today's date, who the CSM is,
# executive sponsor names, site leaders, SMEs, AE, logos, and other roster-style
# placeholders. Filling those does **not** require exports or cross-system lookups.
#
# This script prioritizes gaps where sourcing **performance and operations data** saves
# real time: shortages, CTB / DOI / inventory metrics, platform & product adoption (e.g. Kei),
# Lean/value line items, Jira/Support rollups, CS Report KPIs — anything normally pulled from
# analytics, ERP/MRP snapshots, spreadsheets, or BPO hydrate mappings rather than memory.
#
# The appended summary slide(s) and gap_inventory_rows use **time_saving_hits** (filtered +
# collapsed). Raw **heuristic_hits** remain in JSON for debugging. Use --llm to infer gaps
# the heuristics miss; the model is instructed to ignore clerical placeholders.
# -----------------------------------------------------------------------------
"""Scan slides for **CSM lookup / data-integration** gaps (time savings).

Heuristics + optional LLM classify missing **performance & operations** content (supply
chain, inventory, ERP/MRP-era metrics, adoption). Clerical placeholders (dates, contacts,
logos, meeting logistics) are excluded from the report table and inventory JSON rows.

Summary slide title: **CSM lookup — data to source**.

See ``docs/SLIDE_DATA_GAP_ANALYSIS.md`` for methodology.

Usage:
  python scripts/identify-data-gaps.py
  python scripts/identify-data-gaps.py --presentation PRES_OR_URL [--out gaps.json]

By default resolves the Drive QBR Slides template (``QBR_TEMPLATE_FILE_NAME``, same as QBR runs).
Append summary slides **only** when ``--presentation`` targets a deck copy (`--write-summary-slide`).

Requires ``GOOGLE_QBR_GENERATOR_FOLDER_ID``, Google credentials; ``--llm`` needs API keys per ``src.config``.

Default JSON is **compact**: only ``replacements[]`` with ``slide``, ``find``, ``replace.value``,
``replace.format``, optional ``replace.display``, optional ``source``. Use ``--verbose-json`` for
full per-slide ``heuristic_hits`` / ``time_saving_hits`` plus ``gap_inventory_rows``.
"""
from __future__ import annotations

import argparse
import json
import re
import secrets
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_PLACEHOLDER_BRACKETS = re.compile(r"\[[^\]]{1,200}\]")
_HYDRATE_TOKENS = re.compile(
    r"\[000\]|\[\$000\]|\[00/00/00\]|\[00%\]|\[\?\?\?\]", re.IGNORECASE
)
_LOREM = re.compile(r"\blorem ipsum\b", re.IGNORECASE)
_TBD = re.compile(r"\b(TBD|TODO|FIXME|FILL IN|fill in|placeholder|sample data)\b", re.IGNORECASE)
_GENERIC_DATE = re.compile(r"202X|Mon 202X|Q\?|QX\b")
_GENERIC_METRIC = re.compile(r"\bxx%|\byy%|N/?A\b", re.IGNORECASE)

# Bracket text CSMs routinely fill from memory / calendar / rolodex — not a data *lookup*.
_CLERICAL_BRACKET_INNER = re.compile(
    r"^(current date|today'?s?\s*date)|"
    r"(^|\b)(executive\s+sponsor|exec\s+sponsor|implementation\s+manager|"
    r"site\s+leader|customer\s+success\s+manager|\bcsm\b|account\s+executive|\bae\b|"
    r"sme\(s\)?|it\s+leader)|"
    r"add\s+logo|logo\s+from|fill\s+in\s+names|"
    r"record\s+this\s+qbr|can\s+we\s+record",
    re.IGNORECASE,
)


def _bracket_inner(span: str) -> str:
    s = (span or "").strip()
    if len(s) >= 2 and s.startswith("[") and s.endswith("]"):
        return s[1:-1].strip()
    return s


def _is_clerical_bracket_placeholder(span: str) -> bool:
    inner = _bracket_inner(span).lower()
    if not inner:
        return True
    # Template IT line variants are roster/policy, not a KPI pull.
    if "it leader" in inner and ("optional" in inner or "recommended" in inner):
        return True
    return bool(_CLERICAL_BRACKET_INNER.search(inner))


def _time_saving_hits(all_hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter/collapse hits for the report: drop clerical noise; one row per slide for N images/charts."""
    out: list[dict[str, Any]] = []
    n_img = sum(1 for h in all_hits if h.get("kind") == "image_or_screenshot")
    n_cht = sum(1 for h in all_hits if h.get("kind") == "embedded_chart")

    for h in all_hits:
        k = h.get("kind")
        if k in ("image_or_screenshot", "embedded_chart"):
            continue
        if k == "bracket_placeholder" and _is_clerical_bracket_placeholder(str(h.get("span") or "")):
            continue
        if k == "lorem":
            continue
        if k == "empty_text":
            continue
        out.append(dict(h))

    if n_cht:
        out.append(
            {
                "kind": "embedded_charts_collapsed",
                "count": n_cht,
                "detail": f"{n_cht} embedded chart(s) — underlying series may need refresh from BI/Sheets/export.",
            }
        )
    if n_img:
        out.append(
            {
                "kind": "images_collapsed",
                "count": n_img,
                "detail": f"{n_img} image(s)/screenshots — may hide KPIs needing re-export or OCR.",
            }
        )
    return out


_LLM_CLERICAL_SKIP = re.compile(
    r"executive\s+sponsor|exec\s+sponsor|site\s+leader|\bcsm\b|account\s+executive|\bae\b|"
    r"implementation\s+manager|\bsme\b|it\s+leader.*optional|current\s+date|today'?s?\s+date|"
    r"add\s+logo|logo\s+from|fill\s+in\s+names|record\s+this\s+qbr|can\s+we\s+record",
    re.IGNORECASE,
)


def _llm_item_is_time_saving(item: dict[str, Any]) -> bool:
    """Exclude LLM suggestions that only describe roster / date / logo logistics."""
    parts = " ".join(
        str(item.get(k) or "")
        for k in ("field_found", "label", "business_concept", "relevant_context", "notes")
    )
    if not parts.strip():
        return False
    return _LLM_CLERICAL_SKIP.search(parts) is None


_GAP_TABLE_HEADERS = [
    "Slide",
    "Field / signal",
    "Suggested datasource",
    "Data type",
    "Formatting",
    "Time / duration",
    "Accuracy / precision",
    "Context / notes",
]

_ROWS_PER_SUMMARY_SLIDE = 7
_TITLE_Y = 24.0
_TABLE_TOP = 72.0
_ROW_H = 26.0
# Slides API: each column width must be ≥ 32 PT (406400 EMU).
_COL_WIDTHS = [36.0, 104.0, 72.0, 56.0, 62.0, 56.0, 62.0, 156.0]


def _tc(val: Any, max_len: int = 260) -> str:
    if isinstance(val, list):
        val = "; ".join(str(x) for x in val if x is not None and str(x).strip())
    t = str(val or "").replace("\r", " ").replace("\n", " ").strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def _presentation_id(arg: str) -> str:
    s = (arg or "").strip()
    m = re.search(r"/presentation/d/([a-zA-Z0-9_-]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9_-]+", s):
        return s
    raise SystemExit(
        f"Could not parse presentation id from {arg!r}. "
        "Use a file id or a full docs.google.com/presentation/d/... URL."
    )


def _heuristic_hits(text: str) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    if not (text or "").strip():
        hits.append({"kind": "empty_text", "detail": "No extractable text on slide"})
        return hits

    for m in _PLACEHOLDER_BRACKETS.finditer(text):
        hits.append({"kind": "bracket_placeholder", "span": m.group(0)[:200]})
    for m in _HYDRATE_TOKENS.finditer(text):
        hits.append({"kind": "hydrate_token", "span": m.group(0)})
    if _LOREM.search(text):
        hits.append({"kind": "lorem", "detail": "Editorial filler (lorem ipsum)"})
    if _TBD.search(text):
        hits.append({"kind": "explicit_stub", "detail": "TBD / fill-in style wording"})
    if _GENERIC_DATE.search(text):
        hits.append({"kind": "generic_date", "detail": "Undated or generic period token"})
    if _GENERIC_METRIC.search(text):
        hits.append({"kind": "generic_metric", "detail": "xx%/yy% or vague metric token"})

    return hits


def _structural_flags(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flags: list[dict[str, Any]] = []
    for it in items:
        t = it.get("type")
        if t == "chart":
            flags.append(
                {
                    "kind": "embedded_chart",
                    "detail": "Linked/static chart — underlying numbers may not match live ERP/report exports.",
                    "element_id": it.get("element_id"),
                }
            )
        elif t == "image":
            flags.append(
                {
                    "kind": "image_or_screenshot",
                    "detail": "Raster/image — metrics inside may be unreadable to this scan.",
                    "element_id": it.get("element_id"),
                }
            )
    return flags


def _slide_bundle(slide: dict[str, Any], slide_index: int) -> dict[str, Any]:
    from src.hydrate_extract import describe_elements, extract_slide_text_elements

    page_elements = slide.get("pageElements") or []
    items = extract_slide_text_elements(page_elements)
    texts = [it.get("text") or "" for it in items if it.get("text")]
    blob = "\n".join(texts)
    hits = _heuristic_hits(blob)
    hits.extend(_structural_flags(items))
    ts_hits = _time_saving_hits(hits)
    return {
        "slide_index": slide_index,
        "object_id": slide.get("objectId"),
        "element_counts": describe_elements(slide),
        "text_blocks": len([x for x in items if x.get("type") in ("shape", "table_cell")]),
        "concat_preview": (blob[:1200] + ("…" if len(blob) > 1200 else "")) if blob else "",
        "heuristic_hits": hits,
        "time_saving_hits": ts_hits,
        "severity_hint": "high"
        if any(
            h.get("kind")
            in (
                "bracket_placeholder",
                "hydrate_token",
                "generic_metric",
                "embedded_charts_collapsed",
                "images_collapsed",
            )
            for h in ts_hits
        )
        else ("medium" if ts_hits else "low"),
    }


def _heuristic_table_row(slide_idx: int, hit: dict[str, Any]) -> list[str]:
    kind = str(hit.get("kind") or "")
    span = hit.get("span") or hit.get("detail") or kind
    field = _tc(span, 220)
    if kind == "embedded_charts_collapsed":
        n = int(hit.get("count") or 0)
        return [
            str(slide_idx),
            _tc(f"Embedded charts (×{n})"),
            _tc("Linked Sheet / BI / manual chart build"),
            _tc("Time series or category breakdown"),
            _tc("Match axis labels & units from source feed"),
            _tc("Same fiscal window as adjacent KPI slides"),
            _tc("Refresh linkage or paste from governed export"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "images_collapsed":
        n = int(hit.get("count") or 0)
        return [
            str(slide_idx),
            _tc(f"Images/screenshots (×{n})"),
            _tc("Upstream screenshot source (ERP, analytics, deck library)"),
            _tc("Raster — often hides metrics"),
            _tc("Prefer live chart or citation of export date"),
            _tc("As-of capture vs quarter under review"),
            _tc("Re-capture if numbers are authoritative"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "bracket_placeholder":
        return [
            str(slide_idx),
            field,
            _tc("Map phrase to hydrate path / CSR / LeanDNA / Pendo / Salesforce export"),
            _tc("Domain-specific KPI or narrative tied to bracket label"),
            _tc("Honor percent/currency/date conventions on slide"),
            _tc("Match QBR fiscal quarter unless slide states otherwise"),
            _tc("Use system-of-record rounding; cite source"),
            _tc("Operational placeholder — typically requires lookup, not roster memory."),
        ]
    if kind == "hydrate_token":
        return [
            str(slide_idx),
            field,
            _tc("BPO hydrate catalog / mapped API field"),
            _tc("Metric, currency, date, or percent per token type"),
            _tc("Follow slide numeric style (decimals, units)"),
            _tc("Match reporting window (e.g. quarter, trailing 90d)"),
            _tc("Use same rounding as source system of record"),
            _tc("Hydration placeholder — bind to data_summary path or YAML mapping."),
        ]
    if kind == "embedded_chart":
        return [
            str(slide_idx),
            _tc("Embedded chart"),
            _tc("Sheets link / BI export"),
            _tc("Time series or breakdown (visual)"),
            _tc("Recreate or refresh chart; label axes & units"),
            _tc("Align period with deck quarter"),
            _tc("Prefer automated refresh over static PNG"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "image_or_screenshot":
        return [
            str(slide_idx),
            _tc("Image / screenshot"),
            _tc("Source system shown in capture (ERP, analytics)"),
            _tc("Raster — may encode KPI screen grabs"),
            _tc("Replace with live chart or cite export snapshot date"),
            _tc("As-of capture vs quarter reviewed"),
            _tc("Re-source if treated as authoritative"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "empty_text":
        return [
            str(slide_idx),
            _tc("(no extractable text)"),
            _tc("Slide definition / speaker notes / attached Sheet"),
            _tc("Unknown — possibly visual-only"),
            _tc("N/A"),
            _tc("N/A"),
            _tc("N/A"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "lorem":
        return [
            str(slide_idx),
            _tc("Lorem ipsum"),
            _tc("N/A — editorial replacement"),
            _tc("Narrative paragraph"),
            _tc("Match tone & reading level of deck"),
            _tc("N/A"),
            _tc("Human-authored"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "explicit_stub":
        return [
            str(slide_idx),
            _tc("TBD / fill-in marker"),
            _tc("Per adjacent slide labels / workshop notes"),
            _tc("Mixed"),
            _tc("Replace stub with final copy or metric"),
            _tc("Per stakeholder agreement"),
            _tc("Confirm sign-off owner"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "generic_date":
        return [
            str(slide_idx),
            _tc("Generic date token"),
            _tc("Calendar / FP&A / quarter definitions"),
            _tc("Date or period label"),
            _tc("ISO or company-standard fiscal label"),
            _tc("Explicit fiscal quarter or date range"),
            _tc("Must match rest of deck"),
            _tc(hit.get("detail", "")),
        ]
    if kind == "generic_metric":
        return [
            str(slide_idx),
            _tc("Generic metric token (xx% / N/A)"),
            _tc("IBP / ERP / CS export / analytics"),
            _tc("Percent, count, or currency"),
            _tc("Units & decimal places per KPI standard"),
            _tc("Same window as peer KPIs on slide"),
            _tc("Validate against source query"),
            _tc(hit.get("detail", "")),
        ]
    return [
        str(slide_idx),
        field,
        _tc("Infer from slide title & adjacent bullets"),
        _tc("Unknown"),
        _tc("Clarify with domain owner"),
        _tc("Align to deck period"),
        _tc("Medium — needs validation"),
        _tc(kind),
    ]


def _llm_table_row(slide_idx: int, item: dict[str, Any]) -> list[str]:
    field = item.get("field_found") or item.get("label") or "(unspecified)"
    src = item.get("suggested_datasource")
    if src is None and item.get("likely_sources") is not None:
        src = item.get("likely_sources")
    return [
        str(slide_idx),
        _tc(field),
        _tc(src),
        _tc(item.get("datatype") or item.get("business_concept")),
        _tc(item.get("formatting")),
        _tc(item.get("time_scope_duration")),
        _tc(item.get("accuracy_precision")),
        _tc(item.get("relevant_context") or item.get("notes")),
    ]


def _infer_format_from_bracket(find: str) -> str:
    f = find.lower()
    if "%" in find or "ctb" in f or "doi" in f or "yield" in f or "promoter" in f:
        return "percent"
    if "$" in find or "arr" in f or "usd" in f or ("value" in f and "roi" in f):
        return "currency"
    if re.search(r"\b20\d{2}\b|\bqx\b|\bquarter\b|fiscal|ytd", f):
        return "date_or_period"
    return "text"


def _format_from_hydrate_token(span: str) -> str:
    ul = span.strip().upper()
    if "00%" in ul:
        return "percent"
    if "$" in span and "000" in ul:
        return "currency"
    if "00/00/00" in ul:
        return "date"
    if "[000]" in span or "[???]" in span:
        return "numeric_placeholder"
    return "unknown"


def _compact_replace(value: Any, format_id: str, display: str | None = None) -> dict[str, Any]:
    r: dict[str, Any] = {"value": value, "format": format_id}
    if display:
        r["display"] = display
    return r


def _compact_from_heuristic(slide_idx: int, h: dict[str, Any]) -> dict[str, Any]:
    kind = str(h.get("kind") or "")
    span = str(h.get("span") or h.get("detail") or kind).strip()
    find = _tc(span, 280) if span else kind

    if kind == "bracket_placeholder":
        return {
            "slide": slide_idx,
            "find": find,
            "replace": _compact_replace(None, _infer_format_from_bracket(find)),
            "source": "map_to_hydrate_or_export",
        }
    if kind == "hydrate_token":
        fmt = _format_from_hydrate_token(span or find)
        return {
            "slide": slide_idx,
            "find": find,
            "replace": _compact_replace(None, fmt),
            "source": "bpo_data_summary_or_qbr_mappings",
        }
    if kind == "generic_metric":
        return {
            "slide": slide_idx,
            "find": "(generic xx% / N/A style token)",
            "replace": _compact_replace(None, "percent_or_text", "Resolve from IBP / ERP / CS export / analytics"),
            "source": "ibp_or_erp_or_cs_export",
        }
    if kind == "generic_date":
        return {
            "slide": slide_idx,
            "find": "(generic fiscal period placeholder)",
            "replace": _compact_replace(None, "date_or_period", "Align to deck fiscal quarter labels"),
            "source": "fp_a_or_calendar",
        }
    if kind == "explicit_stub":
        return {
            "slide": slide_idx,
            "find": "(TBD / template stub nearby)",
            "replace": _compact_replace(None, "unknown", _tc(h.get("detail") or "", 160)),
            "source": None,
        }
    if kind == "embedded_charts_collapsed":
        n = int(h.get("count") or 0)
        find = f"Embedded charts (×{n})"
        return {
            "slide": slide_idx,
            "find": find,
            "replace": _compact_replace(None, "chart", "Refresh data series or reconnect Sheet/BI"),
            "source": "sheets_bi_or_manual",
        }
    if kind == "images_collapsed":
        n = int(h.get("count") or 0)
        find = f"Images/screenshots (×{n})"
        return {
            "slide": slide_idx,
            "find": find,
            "replace": _compact_replace(None, "raster_visual", "Re-export KPIs from system of record if authoritative"),
            "source": "screenshot_upstream",
        }

    rf = _compact_replace(None, kind or "unknown", _tc(h.get("detail") or "", 200))
    return {"slide": slide_idx, "find": find, "replace": rf, "source": None}


def _compact_from_llm(slide_idx: int, item: dict[str, Any]) -> dict[str, Any]:
    field = str(item.get("field_found") or item.get("label") or "").strip() or "(unspecified)"
    datatype = _tc(item.get("datatype") or item.get("business_concept") or "unknown", 120)
    disp_parts: list[str] = []
    if item.get("formatting"):
        disp_parts.append(_tc(item.get("formatting"), 200))
    if item.get("time_scope_duration"):
        disp_parts.append(f"period: {_tc(item.get('time_scope_duration'), 120)}")
    if item.get("accuracy_precision"):
        disp_parts.append(f"precision: {_tc(item.get('accuracy_precision'), 120)}")
    display = "; ".join(disp_parts).strip() or ""
    src = item.get("suggested_datasource")
    if src is None and item.get("likely_sources") is not None:
        src = item.get("likely_sources")
    src_s = _tc(src, 140) if src else ""

    rep = _compact_replace(None, datatype, display or None)
    out: dict[str, Any] = {"slide": slide_idx, "find": _tc(field, 280), "replace": rep}
    if src_s:
        out["source"] = src_s
    return out


def _prune_compact_replacement(obj: dict[str, Any]) -> dict[str, Any]:
    """Drop null `source`; omit empty `replace.display`."""
    out = dict(obj)
    if out.get("source") is None:
        out.pop("source", None)
    rep = dict(out.get("replace") or {})
    if not rep.get("display"):
        rep.pop("display", None)
    out["replace"] = rep
    return out


def _build_compact_replacements(bundles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Minimal consumer JSON: slide, find text, replace {value, format, display?}."""
    seen: set[tuple[int, str]] = set()
    rows: list[dict[str, Any]] = []
    for b in bundles:
        sn = int(b["slide_index"])
        for h in b.get("time_saving_hits") or []:
            row = _compact_from_heuristic(sn, h)
            key = (sn, row["find"][:120])
            if key in seen:
                continue
            seen.add(key)
            rows.append(_prune_compact_replacement(row))
        llm = b.get("llm")
        if not isinstance(llm, dict) or llm.get("error"):
            continue
        for item in llm.get("missing_data_items") or []:
            if not isinstance(item, dict):
                continue
            if not _llm_item_is_time_saving(item):
                continue
            row = _compact_from_llm(sn, item)
            key = (sn, row["find"][:120])
            if key in seen:
                continue
            seen.add(key)
            rows.append(_prune_compact_replacement(row))
    return rows


def _collect_gap_rows(bundles: list[dict[str, Any]]) -> list[list[str]]:
    """Flatten **time-saving** hits + filtered LLM items; dedupe by (slide, field prefix)."""
    seen: set[tuple[int, str]] = set()
    out: list[list[str]] = []
    for b in bundles:
        sn = int(b["slide_index"])
        for h in b.get("time_saving_hits") or []:
            row = _heuristic_table_row(sn, h)
            key = (sn, row[1][:96])
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        llm = b.get("llm")
        if not isinstance(llm, dict) or llm.get("error"):
            continue
        for item in llm.get("missing_data_items") or []:
            if not isinstance(item, dict):
                continue
            if not _llm_item_is_time_saving(item):
                continue
            row = _llm_table_row(sn, item)
            key = (sn, row[1][:96])
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
    return out


def _llm_analyze_slide(
    client: Any,
    model: str,
    bundle: dict[str, Any],
) -> dict[str, Any]:
    from src.llm_utils import _llm_create_with_retry, _strip_json_code_fence

    system = (
        "You analyze Google Slides **text extracts** from B2B customer QBR/deck content (discrete "
        "manufacturing, inventory, supply chain, ERP/MRP-era operations).\n\n"
        "**Goal — CSM *time savings*:** Only surface gaps where gathering **performance or operations "
        "data** requires lookups, exports, or integrations (things the CSM cannot reliably type from "
        "memory).\n\n"
        "**DO NOT list** items that only save typing for things the CSM already knows locally: today's "
        "date, participant names/contacts ([Executive sponsor], [CSM], [AE], [Site leader], SMEs, logos, "
        "meeting housekeeping). Return **missing_data_items: []** when the slide’s only omissions are "
        "those clerical placeholders.\n\n"
        "**DO prioritize:** shortage / inventory / CTB-style metrics, DOI backwards or similar supply "
        "metrics, Lean or value-line savings, adoption & depth (including product analytics/chatbot/KPI "
        "usage if implied), CSR/Data Export platform health rows, SF ARR-style facts when hinted, Jira "
        "portfolio counts, hydrate-style `[???]`/`[00%]` tokens adjacent to KPI language, unresolved "
        "charts/images that likely carry KPIs.\n\n"
        "For **each retained** gap, specify:\n"
        "- **suggested_datasource**: ERP, MRP, WMS, IBP/planning, CS Report export, Salesforce, LeanDNA, "
        "Pendo/analytics, Jira aggregates, Sheets/BI linkage, etc.\n"
        "- **datatype**: currency, percent, count, trend, categorical site breakdown, narrative with "
        "cited metric, …\n"
        "- **formatting**: decimals, units, chart vs table, footnote discipline.\n"
        "- **time_scope_duration**: fiscal quarter, trailing 90d, as-of snapshot, …\n"
        "- **accuracy_precision**: enterprise vs site rollup, audited vs directional, rounding rules.\n"
        "- **relevant_context**: why this saves CSM cycles vs trivial edits.\n\n"
        "RULES:\n"
        "- Output valid JSON only (no markdown fences).\n"
        "- Never invent customer KPI values.\n"
        "- **field_found**: quote the template hole / bullet that implies the lookup.\n"
        "- confidence: high | medium | low.\n\n"
        'Schema: {"slide_index": int, "inferred_purpose": string, '
        '"missing_data_items": [ {"field_found": string, "label": string, '
        '"business_concept": string, "suggested_datasource": string, "datatype": string, '
        '"formatting": string, "time_scope_duration": string, "accuracy_precision": string, '
        '"relevant_context": string, "confidence": string, "notes": string} ], '
        '"hedges": string }\n'
        "(**likely_sources** legacy: you may repeat suggested_datasource as a single-element array in notes "
        "if needed; prefer the explicit fields above.)\n"
    )
    user_payload = {
        "slide_index": bundle["slide_index"],
        "object_id": bundle["object_id"],
        "element_counts": bundle["element_counts"],
        "heuristic_hits": bundle["heuristic_hits"],
        "text_preview": bundle["concat_preview"],
    }
    raw = json.dumps(user_payload, indent=2, default=str)
    resp = _llm_create_with_retry(
        client,
        model=model,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": "Slide bundle JSON:\n" + raw[:14_000]},
        ],
    )
    txt = (resp.choices[0].message.content or "").strip()
    try:
        return json.loads(_strip_json_code_fence(txt))
    except json.JSONDecodeError:
        return {"error": "json_parse_failed", "raw": txt[:2000]}


def _append_gap_inventory_slides(
    slides_svc: Any,
    pres_id: str,
    *,
    deck_slide_count: int,
    table_rows: list[list[str]],
) -> tuple[int, list[str]]:
    """Append summary slide(s). Returns (slides_added, messages)."""
    from src.slide_primitives import simple_table
    from src.slide_requests import append_slide, append_text_box
    from src.slides_theme import MARGIN
    from src.slides_api import presentations_batch_update_chunked

    msgs: list[str] = []
    if not table_rows:
        table_rows = [
            [
                "—",
                "(none)",
                "N/A",
                "N/A",
                "N/A",
                "N/A",
                "N/A",
                "No **lookup-heavy** gaps on scanned slides (clerical placeholders are filtered out). "
                "Re-run with --llm if KPI gaps are under-detected.",
            ]
        ]

    chunks: list[list[list[str]]] = []
    for i in range(0, len(table_rows), _ROWS_PER_SUMMARY_SLIDE):
        chunks.append(table_rows[i : i + _ROWS_PER_SUMMARY_SLIDE])

    reqs: list[dict[str, Any]] = []
    insertion_base = deck_slide_count
    total_parts = len(chunks)

    for part_idx, chunk_rows in enumerate(chunks):
        sid = f"gapscan_{secrets.token_hex(10)}"
        tid = f"{sid}_tbl"
        title_oid = f"{sid}_title"
        insertion = insertion_base + part_idx
        append_slide(reqs, sid, insertion)
        title = (
            f"CSM lookup — data to source ({part_idx + 1}/{total_parts})"
            if total_parts > 1
            else "CSM lookup — data to source"
        )
        append_text_box(reqs, title_oid, sid, MARGIN, _TITLE_Y, 640.0, 44.0, title)
        simple_table(
            reqs,
            tid,
            sid,
            MARGIN,
            _TABLE_TOP,
            list(_COL_WIDTHS),
            _ROW_H,
            list(_GAP_TABLE_HEADERS),
            chunk_rows,
        )

    presentations_batch_update_chunked(slides_svc, pres_id, reqs)
    msgs.append(
        f"Appended {total_parts} summary slide(s) at end of deck (starting after slide {deck_slide_count})."
    )
    msgs.append(f"Presentation: https://docs.google.com/presentation/d/{pres_id}/edit")
    return total_parts, msgs


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Find slide gaps that cost CSM lookup time (KPIs, exports); "
            "defaults to Drive QBR template; optional summary table append on copied decks."
        ),
    )
    ap.add_argument(
        "--presentation",
        metavar="ID_OR_URL",
        help=(
            "Scan this presentation instead of the canonical QBR template on Drive "
            "(id or docs.google.com/presentation/d/... URL)"
        ),
    )
    ap.add_argument(
        "--write-summary-slide",
        action="store_true",
        help=(
            "Append gap summary slide(s) at deck end "
            "(only with --presentation; never mutates the canonical template)"
        ),
    )
    ap.add_argument("--out", "-o", metavar="FILE", help="Write JSON report to file")
    ap.add_argument(
        "--max-slides",
        type=int,
        default=None,
        metavar="N",
        help="Only analyze first N slides of the deck (default: all)",
    )
    ap.add_argument(
        "--llm",
        action="store_true",
        help="LLM pass per analyzed slide (uses LLM_MODEL from config)",
    )
    ap.add_argument(
        "--no-write-summary-slide",
        action="store_true",
        help="Do not append summary slide(s); with --presentation, overrides the default append",
    )
    ap.add_argument(
        "--verbose-json",
        action="store_true",
        help="Include full per-slide payloads, gap_inventory_rows, and metadata (default is compact replacements only)",
    )
    args = ap.parse_args()

    presentation_arg = (args.presentation or "").strip()
    if presentation_arg:
        pres_id = _presentation_id(presentation_arg)
        template_scan = False
    else:
        from src.drive_config import resolve_qbr_template_presentation_id

        pres_id = resolve_qbr_template_presentation_id()
        template_scan = True

    if template_scan:
        write_summary_slides = False
        if args.write_summary_slide:
            raise SystemExit(
                "Refusing to append summary slides to the canonical QBR template on Drive. "
                "Use --presentation on a copied deck plus --write-summary-slide, "
                "or omit --write-summary-slide for JSON-only output when scanning the template."
            )
    else:
        if args.write_summary_slide and args.no_write_summary_slide:
            raise SystemExit("Use either --write-summary-slide or --no-write-summary-slide, not both.")
        write_summary_slides = args.write_summary_slide or not args.no_write_summary_slide

    from src.slides_api import _get_service

    slides_svc, _drive, _ = _get_service()
    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    title = pres.get("title") or "(untitled)"
    all_slides = pres.get("slides") or []
    deck_slide_count = len(all_slides)

    scan_slides = all_slides[: args.max_slides] if args.max_slides is not None else all_slides

    bundles: list[dict[str, Any]] = []
    for i, slide in enumerate(scan_slides):
        bundles.append(_slide_bundle(slide, i + 1))

    scan_bundles: list[dict[str, Any]] = list(bundles)

    if args.llm:
        from src.config import LLM_MODEL, llm_client

        client = llm_client()
        enriched: list[dict[str, Any]] = []
        for b in bundles:
            row = dict(b)
            try:
                row["llm"] = _llm_analyze_slide(client, LLM_MODEL, b)
            except Exception as e:
                row["llm"] = {"error": str(e)}
            enriched.append(row)
        scan_bundles = enriched

    replacements = _build_compact_replacements(scan_bundles)
    gap_rows = _collect_gap_rows(scan_bundles)

    report: dict[str, Any] = {
        "presentation_id": pres_id,
        "title": title,
        "replacement_count": len(replacements),
        "replacements": replacements,
    }
    if args.verbose_json:
        report["report_focus"] = "csm_lookup_time_savings"
        report["deck_slide_count"] = deck_slide_count
        report["slides_analyzed"] = len(scan_bundles)
        report["gap_table_headers"] = list(_GAP_TABLE_HEADERS)
        report["slides"] = scan_bundles
        report["gap_inventory_rows"] = [
            dict(zip(_GAP_TABLE_HEADERS, r, strict=True)) for r in gap_rows
        ]
        if args.llm:
            report["llm_model"] = LLM_MODEL
    elif args.llm:
        report["llm_model"] = LLM_MODEL

    report["scan_root"] = "qbr_drive_template" if template_scan else "presentation_override"

    status_msgs: list[str] = []
    if write_summary_slides:
        n_added, status_msgs = _append_gap_inventory_slides(
            slides_svc,
            pres_id,
            deck_slide_count=deck_slide_count,
            table_rows=gap_rows,
        )
        report["summary_slides_appended"] = n_added
        for m in status_msgs:
            print(m, file=sys.stderr)

    text = json.dumps(report, indent=2, default=str)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"Wrote {args.out}", file=sys.stderr)
    print(text)


if __name__ == "__main__":
    main()
