"""Evaluate and hydrate slides dropped in the new-slides Drive folder.

Evaluate: reads presentations from a 'new-slides' subfolder, exports thumbnails,
extracts text/elements, and asks GPT-4o to assess reproducibility.

Hydrate: classifies each slide in a source deck against our builder types,
then regenerates the deck using live customer data from Pendo/Jira/CS Report.
"""

from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests as _requests

from .config import GOOGLE_DRIVE_FOLDER_ID, LLM_MODEL, LLM_MODEL_FAST, llm_client, logger
from .slides_client import _get_service, set_speaker_notes, SLIDE_DATA_REQUIREMENTS

NEW_SLIDES_FOLDER_NAME = "new-slides"

# Slide analysis cache — avoid re-calling the LLM for the same slide content.
# Bump CACHE_VERSION when the classification prompt or slide types change.
_SLIDE_CACHE_VERSION = 1


def _slide_cache_dir() -> Path:
    """Directory for persisted slide analysis cache (classification, adapt)."""
    root = Path(__file__).resolve().parent.parent
    return root / ".slide_cache"


def _slide_content_hash(thumb_b64: str | None, text_snapshot: str = "") -> str | None:
    """Stable hash for cache key. Thumbnail-only when available; else text so thumb-less slides can still cache."""
    if thumb_b64:
        raw = base64.b64decode(thumb_b64, validate=True)
        return hashlib.sha256(raw).hexdigest()
    if text_snapshot:
        return hashlib.sha256(text_snapshot.encode("utf-8")).hexdigest()
    return None


def _get_cached_classification(cache_key: str) -> dict | None:
    """Return cached classification result if present and version matches."""
    d = _slide_cache_dir() / "classification"
    path = d / f"{cache_key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("_version") != _SLIDE_CACHE_VERSION:
            return None
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception:
        return None


def _set_cached_classification(cache_key: str, result: dict) -> None:
    """Persist classification result for this cache key."""
    d = _slide_cache_dir() / "classification"
    d.mkdir(parents=True, exist_ok=True)
    out = {"_version": _SLIDE_CACHE_VERSION, **result}
    (d / f"{cache_key}.json").write_text(json.dumps(out, indent=0), encoding="utf-8")


def _get_cached_adapt(cache_key: str) -> list[dict] | None:
    """Return cached adapt replacements if present and version matches."""
    d = _slide_cache_dir() / "adapt"
    path = d / f"{cache_key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("_version") != _SLIDE_CACHE_VERSION:
            return None
        return data.get("replacements", [])
    except Exception:
        return None


def _set_cached_adapt(cache_key: str, replacements: list[dict]) -> None:
    """Persist adapt replacements for this cache key (values are resolved at read time)."""
    d = _slide_cache_dir() / "adapt"
    d.mkdir(parents=True, exist_ok=True)
    out = {"_version": _SLIDE_CACHE_VERSION, "replacements": replacements}
    (d / f"{cache_key}.json").write_text(json.dumps(out, indent=0, default=str), encoding="utf-8")


def _resolve_cached_replacements(cached: list[dict], data_summary: dict) -> list[dict]:
    """For cached replacements with mapped=true, set new_value from current data_summary.
    Tries to preserve format (e.g. "31 sites" -> "14 sites") when original has a trailing suffix.
    """
    import re as _re
    out = []
    for r in list(cached):
        r = dict(r)
        if r.get("mapped") and r.get("field"):
            key = r["field"].strip().replace(" ", "_").replace("-", "_").lower()
            if key in data_summary:
                val = data_summary[key]
                if isinstance(val, (list, dict)):
                    r["new_value"] = str(val)[:200]
                else:
                    raw = str(val) if val is not None else ""
                    orig = r.get("original", "")
                    # Preserve suffix from original (e.g. "31 sites" -> "14 sites")
                    m = _re.match(r"^[\d.,\s$€£%]+", orig)
                    suffix = (orig[m.end():].strip() if m else "").strip()
                    r["new_value"] = f"{raw} {suffix}".strip() if suffix else raw
        out.append(r)
    return out


# ── Broader slide analysis (data ask + purpose) for future-proof cache ─────────
# Bump when we change the analysis schema so old entries are ignored.
_SLIDE_ANALYSIS_CACHE_VERSION = 1

# Canonical data keys we can resolve from report/data_summary. LLM uses these or adds slugs.
CANONICAL_DATA_KEYS = (
    "customer_name", "report_date", "quarter", "quarter_start", "quarter_end",
    "total_users", "active_users", "total_sites", "active_sites", "health_score",
    "site_details", "cs_health_sites", "support", "salesforce", "platform_value",
    "supply_chain",
)


def _get_cached_slide_analysis(cache_key: str) -> dict | None:
    """Return cached broad analysis (data_ask, purpose, slide_type) if present and version matches."""
    d = _slide_cache_dir() / "analysis"
    path = d / f"{cache_key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("_version") != _SLIDE_ANALYSIS_CACHE_VERSION:
            return None
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception:
        return None


def _set_cached_slide_analysis(cache_key: str, analysis: dict) -> None:
    """Persist broad slide analysis for this cache key."""
    d = _slide_cache_dir() / "analysis"
    d.mkdir(parents=True, exist_ok=True)
    out = {"_version": _SLIDE_ANALYSIS_CACHE_VERSION, **analysis}
    (d / f"{cache_key}.json").write_text(json.dumps(out, indent=0, default=str), encoding="utf-8")


def _analyze_slide_broad(client, text: str, elements: dict, thumb_b64: str | None,
                         slide_num: int, total: int, pres_name: str) -> dict:
    """One-time broad analysis: what data does this slide ask for, and what is its purpose?

    Returns data_ask (list of {key, example_from_slide}), purpose, slide_type, title, etc.
    Cached so we don't re-run when we add data sources or capabilities later.
    """
    builder_list = "\n".join(f"  - {k}: {v}" for k, v in _BUILDER_DESCRIPTIONS.items())
    keys_list = ", ".join(CANONICAL_DATA_KEYS)

    system = (
        "You are analyzing a slide from a customer QBR deck to capture (1) what DATA it asks for "
        "and (2) its PURPOSE. This analysis will be cached so we can re-hydrate the slide later "
        "as we add data sources — be specific and complete.\n\n"
        "DATA ASK: List every piece of data the slide displays or expects, as specifically as possible. "
        "For each item use a canonical key if it matches our schema, otherwise a short slug (lowercase, underscores).\n"
        f"Canonical keys we support: {keys_list}. "
        "Also support: nps_score, contract_value, arr, budget, timeline, and any other slug that describes the data.\n"
        "For each data item return: key (canonical or slug), example_from_slide (the exact or representative text from the slide, e.g. '31 sites'). "
        "Include embedded charts/images as data items: key '_embedded_chart' or '_embedded_image', example_from_slide the marker text.\n\n"
        "PURPOSE: In one sentence, what is this slide trying to communicate? "
        "E.g. 'Account overview with key metrics' or 'Inventory trends by site'. "
        "This helps us later offer a data-equivalent simpler slide if we cannot reproduce the exact one.\n\n"
        f"SLIDE TYPE: Choose from our builders: {builder_list}\n"
        "Use the same rules as classification (custom for budget/roadmap/timelines; bespoke_deployment only for site table + health).\n\n"
        "Return JSON:\n"
        "  data_ask: [{ key, example_from_slide }, ...]\n"
        "  purpose: string (one sentence)\n"
        "  slide_type: one of the builder types above\n"
        "  title: slide title or section name\n"
        "  reasoning: 1 sentence for slide_type choice\n"
        "  custom_sections: (only if slide_type='custom') [{header, body}] for text sections, max 5, body max 200 chars.\n"
    )

    parts: list[dict] = []
    if thumb_b64:
        parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"},
        })
    parts.append({"type": "text", "text": (
        f"Presentation: {pres_name}\nSlide {slide_num}/{total}\n\n"
        f"Extracted text:\n{text or '(no text)'}\n\n"
        f"Elements: {json.dumps(elements)}\n\n"
        "Analyze: data_ask (all data this slide asks for), purpose, slide_type, title."
    )})

    resp = _llm_create_with_retry(client,
        model=LLM_MODEL,
        temperature=0,
        max_tokens=4096,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": parts},
        ],
    )
    return json.loads(resp.choices[0].message.content)


def _resolve_data_ask_to_replacements(data_ask: list[dict], data_summary: dict,
                                       text_elements: list[dict]) -> list[dict]:
    """Turn cached data_ask into replacement list using current data_summary and slide text.

    Matches data_ask items to text_elements by example_from_slide; resolves key to value from data_summary.
    """
    import re as _re
    replacements: list[dict] = []
    # Build set of text snippets we can match (from slide)
    element_texts = [el.get("text", "") for el in text_elements if el.get("text")]

    for item in data_ask:
        key = (item.get("key") or "").strip().replace(" ", "_").replace("-", "_").lower()
        example = (item.get("example_from_slide") or "").strip()
        if not key:
            continue
        # Special keys: visual elements we cannot replace with data
        if key in ("_embedded_chart", "_embedded_image") or key.startswith("_embedded"):
            replacements.append({
                "original": example or f"({key})",
                "new_value": "[CHART — data cannot be auto-updated]" if "chart" in key else "[STATIC IMAGE — contains data that cannot be auto-updated]",
                "mapped": False,
                "field": key,
            })
            continue
        # Find best matching element text (exact or contains)
        original = None
        for et in element_texts:
            if example and example in et:
                original = example
                break
            if et and not original and (example in et or et in example):
                original = et
        if not original and example:
            original = example
        # Resolve value from data_summary
        if key in data_summary:
            val = data_summary[key]
            if isinstance(val, (list, dict)):
                new_value = str(val)[:200]
            else:
                raw = str(val) if val is not None else ""
                # Preserve suffix from example (e.g. "31 sites" -> "14 sites")
                m = _re.match(r"^[\d.,\s$€£%]+", original or "")
                suffix = (original[m.end():].strip() if m and original else "").strip()
                new_value = f"{raw} {suffix}".strip() if suffix else raw
            replacements.append({
                "original": original or example or key,
                "new_value": new_value,
                "mapped": True,
                "field": key,
            })
        else:
            # No current source for this key — placeholder so we mark slide incomplete
            if original or example:
                replacements.append({
                    "original": original or example,
                    "new_value": "[???]",
                    "mapped": False,
                    "field": key,
                })
    return replacements


# Company/vendor name (us). Never treat as the customer when detecting from titles like "Safran & LeanDNA".
COMPANY_NAMES_FOR_DETECT: frozenset[str] = frozenset({"leandna"})  # LeanDNA (company); "Leandna" typo normalizes here

# ── Capability inventory (fed to the evaluator LLM) ──

DATA_SOURCES: dict[str, list[str]] = {
    "Pendo": [
        "engagement tiers (power/core/casual/dormant)", "active user counts & rates (7d/30d)",
        "page views & feature usage ranked", "visitor roles & departments",
        "champion (most active) & at-risk (dormant) user lists with emails",
        "site-level metrics (visitors, events, minutes, last-active)",
        "export behavior (counts by feature, by user, top exporters)",
        "Kei AI chatbot adoption & executive usage",
        "guide engagement (seen/dismissed/advanced rates, per-guide)",
        "customer list with sizing & activity ranking",
        "behavioral depth (read/write/collab breakdown by feature category)",
        "cohort benchmarking (median active rates by manufacturing vertical)",
    ],
    "Jira / JSM": [
        "HELP project tickets (open/resolved/total, by priority & status)",
        "SLA metrics (TTFR, TTR, breach rate, % measured)",
        "ticket sentiment (positive/neutral/negative/unrated)",
        "request channel mix (portal/email/internal)",
        "LEAN project engineering pipeline (open/shipped by priority)",
        "ER project enhancement requests (open/shipped/declined, by priority)",
    ],
    "CS Report (Google Sheets export)": [
        "health status (GREEN/YELLOW/RED) per customer/site",
        "CTB%, CTC%, component availability",
        "shortage counts per site",
        "inventory values, days of inventory (DOI), excess inventory",
        "late PO counts & values",
        "savings achieved, open intelligent-action value",
        "recommendations created, POs placed",
        "daily & weekly active buyer counts & percentages",
    ],
    "teams.yaml (local config)": [
        "CSM / AE / SE team roster per customer (manually maintained)",
    ],
    "cohorts.yaml (local config)": [
        "manufacturing cohort classification per customer (e.g. Aerospace, Automotive)",
    ],
}

SLIDE_BUILDING_CAPABILITIES: list[str] = [
    "Text boxes — configurable font family, size, color, bold/italic, alignment",
    "Tables — header rows, per-cell background color, custom border weight, column widths",
    "Colored rectangles — metric cards, status badges, progress-bar fills",
    "Solid background fills on slides",
    "Two-column and multi-column layouts with precise pt positioning",
    "Dynamic content fitting within a protected BODY_BOTTOM margin",
    "Number formatting (abbreviation: 1.2M, $3.4K, 42.1%)",
    "Branded color palette (navy #081c33, blue #009aff, teal #38c0ce, mint #aefff6, etc.)",
    "Fonts: Source Sans Pro, IBM Plex Serif, Source Sans 3 (monospace)",
    "Auto-skip slides when data is empty (no half-blank slides)",
]

KNOWN_LIMITATIONS: list[str] = [
    "No embedded raster charts (bar, line, pie) — we build metric cards and tables instead. "
    "Matplotlib could render to PNG and be inserted as an image, but this is not wired up yet.",
    "No image insertion from external URLs or Drive (only text shapes, rectangles, and tables).",
    "No Salesforce data yet (ARR, renewal dates, contacts, opportunity pipeline — planned).",
    "No animations, transitions, or speaker notes.",
    "Fixed 720×405 pt (standard 16:9) slide canvas.",
    "No grouped/layered elements — every element is a flat shape on the slide.",
]

EXISTING_SLIDE_TYPES: list[str] = sorted(SLIDE_DATA_REQUIREMENTS.keys())


_print_context = "bpo"  # overridden per command

def _print(*args, **kwargs):
    """Log and print with immediate flush so output appears in real time."""
    end = kwargs.pop("end", "\n")
    sep = kwargs.pop("sep", " ")
    msg = sep.join(str(a) for a in args)
    logger.info("%s: %s", _print_context, msg.rstrip())
    print(msg, end=end, flush=True)


# ── Helpers ──

def _find_new_slides_folder() -> str | None:
    """Find the 'new-slides' subfolder inside the configured Drive folder."""
    if not GOOGLE_DRIVE_FOLDER_ID:
        logger.warning("GOOGLE_DRIVE_FOLDER_ID not set — cannot locate new-slides folder")
        return None

    _x, drive, _sh = _get_service()
    q = (
        f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents "
        f"and name = '{NEW_SLIDES_FOLDER_NAME}' "
        "and mimeType = 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )
    results = drive.files().list(q=q, fields="files(id, name)", pageSize=5).execute()
    files = results.get("files", [])
    if not files:
        logger.warning("No '%s' folder found in Drive — nothing to process. "
                       "Create it inside your GOOGLE_DRIVE_FOLDER_ID folder.", NEW_SLIDES_FOLDER_NAME)
        return None
    return files[0]["id"]


_PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
_GSLIDES_MIME = "application/vnd.google-apps.presentation"


def _convert_pptx_to_slides(drive, file_id: str, name: str, folder_id: str) -> str:
    """Copy a .pptx file into the same folder as a native Google Slides presentation.

    Returns the new Google Slides file ID.
    """
    # Download the pptx bytes
    import io
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    from googleapiclient.http import MediaIoBaseDownload
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)

    # Re-upload with conversion to Google Slides
    from googleapiclient.http import MediaIoBaseUpload
    base_name = name.rsplit(".", 1)[0]  # strip .pptx
    media = MediaIoBaseUpload(fh, mimetype=_PPTX_MIME, resumable=True)
    converted = drive.files().create(
        body={
            "name": base_name,
            "mimeType": _GSLIDES_MIME,
            "parents": [folder_id],
        },
        media_body=media,
        fields="id,name",
    ).execute()
    _print(f"Converted '{name}' → Google Slides '{base_name}' (id: {converted['id']})")
    return converted["id"], base_name


def _list_presentations(folder_id: str) -> list[dict[str, str]]:
    """List Google Slides presentations in a folder, resolving shortcuts.

    .pptx files are automatically converted to Google Slides on the fly.
    """
    _x, drive, _sh = _get_service()
    q = f"'{folder_id}' in parents and trashed = false"
    results = drive.files().list(
        q=q,
        fields="files(id, name, mimeType, shortcutDetails)",
        pageSize=50,
    ).execute()

    presentations: list[dict[str, str]] = []
    for f in results.get("files", []):
        mime = f.get("mimeType", "")
        if mime == _GSLIDES_MIME:
            presentations.append({"id": f["id"], "name": f["name"]})
        elif mime == _PPTX_MIME:
            try:
                new_id, new_name = _convert_pptx_to_slides(drive, f["id"], f["name"], folder_id)
                presentations.append({"id": new_id, "name": new_name})
            except Exception as e:
                _print(f"Could not convert '{f['name']}' to Google Slides: {e}")
        elif mime == "application/vnd.google-apps.shortcut":
            target = f.get("shortcutDetails", {})
            if target.get("targetMimeType") == _GSLIDES_MIME:
                presentations.append({"id": target["targetId"], "name": f["name"]})
    return presentations


def _get_slide_thumbnail_url(slides_svc, pres_id: str, page_id: str) -> str:
    """Get the thumbnail content URL for a slide (main-thread only — not thread-safe).

    The Google API client (httplib2) is not thread-safe, so this must be called
    from a single thread.  Use _download_thumbnail_b64 to fetch the image bytes
    in a worker thread.
    """
    thumb = slides_svc.presentations().pages().getThumbnail(
        presentationId=pres_id,
        pageObjectId=page_id,
        thumbnailProperties_thumbnailSize="LARGE",
    ).execute()
    return thumb["contentUrl"]


def _download_thumbnail_b64(url: str, max_retries: int = 3) -> str:
    """Download a thumbnail from a pre-fetched URL and return base64-encoded PNG.

    Safe to call from worker threads — uses requests which is thread-safe.
    Retries on SSL/network errors.
    """
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = _requests.get(url, timeout=30)
            resp.raise_for_status()
            return base64.b64encode(resp.content).decode()
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    raise last_err


def _get_slide_thumbnail_b64(slides_svc, pres_id: str, page_id: str) -> str:
    """Convenience wrapper: get URL then download. Only safe from a single thread."""
    url = _get_slide_thumbnail_url(slides_svc, pres_id, page_id)
    return _download_thumbnail_b64(url)


def _extract_text(element: dict) -> list[str]:
    """Recursively extract text runs from a page element."""
    texts: list[str] = []

    # Shape text
    shape_text = element.get("shape", {}).get("text", {})
    for te in shape_text.get("textElements", []):
        content = te.get("textRun", {}).get("content", "").strip()
        if content:
            texts.append(content)

    # Table cells
    table = element.get("table", {})
    for row in table.get("tableRows", []):
        for cell in row.get("tableCells", []):
            for te in cell.get("text", {}).get("textElements", []):
                content = te.get("textRun", {}).get("content", "").strip()
                if content:
                    texts.append(content)

    # Groups
    group = element.get("elementGroup", {})
    for child in group.get("children", []):
        texts.extend(_extract_text(child))

    return texts


def _describe_elements(slide: dict) -> dict[str, Any]:
    """Summarize the visual element types on a slide."""
    counts = {"text_boxes": 0, "tables": 0, "images": 0, "shapes": 0, "charts": 0}
    for el in slide.get("pageElements", []):
        if "table" in el:
            counts["tables"] += 1
        elif "image" in el:
            counts["images"] += 1
        elif "sheetsChart" in el:
            counts["charts"] += 1
        elif "shape" in el:
            if el["shape"].get("text", {}).get("textElements"):
                counts["text_boxes"] += 1
            else:
                counts["shapes"] += 1
        elif "elementGroup" in el:
            counts["shapes"] += 1
    return counts


def _build_capability_context() -> str:
    """Build a text summary of our current capabilities for the LLM."""
    lines = ["# Current Capabilities\n"]

    lines.append("## Data Sources")
    for src, fields in DATA_SOURCES.items():
        lines.append(f"\n### {src}")
        for f in fields:
            lines.append(f"  - {f}")

    lines.append("\n## Slide Building")
    for cap in SLIDE_BUILDING_CAPABILITIES:
        lines.append(f"  - {cap}")

    lines.append("\n## Existing Slide Types")
    for st in EXISTING_SLIDE_TYPES:
        reqs = SLIDE_DATA_REQUIREMENTS.get(st, [])
        lines.append(f"  - {st}: needs [{', '.join(reqs)}]")

    lines.append("\n## Known Limitations")
    for lim in KNOWN_LIMITATIONS:
        lines.append(f"  - {lim}")

    return "\n".join(lines)


# ── Main evaluation (data-centric: collect analysis, deduce reproducibility at render) ──

# Keys we can currently fill from report/data_summary. Reproducibility is derived from data_ask vs this set.
_AVAILABLE_DATA_KEYS = frozenset(CANONICAL_DATA_KEYS)


def _cache_hit_rate_line(label: str, hits: int, total: int, **extra: int) -> str:
    """Single log line for cache effectiveness."""
    if total <= 0:
        return f"{label}: no slides"
    pct = 100.0 * hits / total
    parts = [f"{hits}/{total} ({pct:.0f}%)"]
    for k, v in sorted(extra.items()):
        if v:
            parts.append(f"{k}={v}")
    return f"{label}: " + ", ".join(parts)


def _derive_reproducibility(analysis: dict) -> dict:
    """Derive feasibility, gaps, and summary from cached data_ask vs current available keys.

    No LLM call — reproducibility is computed at report/render time from the same
    analysis we use for hydrate.
    """
    data_ask = analysis.get("data_ask") or []
    available_keys = _AVAILABLE_DATA_KEYS
    data_needed: list[dict] = []
    gaps: list[str] = []

    for item in data_ask:
        key = (item.get("key") or "").strip().replace(" ", "_").replace("-", "_").lower()
        if not key:
            continue
        # Visual elements we cannot auto-fill (charts, static images)
        if key.startswith("_embedded"):
            data_needed.append({
                "source": "slide",
                "fields": key,
                "available": False,
                "note": "embedded visual — cannot auto-update",
            })
            gaps.append(f"Embedded visual ({key})")
            continue
        available = key in available_keys
        data_needed.append({
            "source": "report" if available else "—",
            "fields": key,
            "available": available,
            "note": item.get("example_from_slide", ""),
        })
        if not available:
            gaps.append(key)

    n_total = len(data_ask)
    n_available = sum(1 for d in data_needed if d.get("available"))
    if n_total == 0:
        feasibility = "fully reproducible"
        summary = "Static slide; no data to fill."
    elif n_available == n_total:
        feasibility = "fully reproducible"
        summary = f"Slide asks for {n_total} data item(s); we have all of them."
    elif n_available > 0:
        feasibility = "partially reproducible"
        summary = f"Slide asks for {n_total} data item(s); we have {n_available}. Gaps: {', '.join(gaps[:5])}{'…' if len(gaps) > 5 else ''}."
    else:
        feasibility = "not reproducible"
        summary = f"Slide asks for {n_total} data item(s); we have none yet. Gaps: {', '.join(gaps[:5])}{'…' if len(gaps) > 5 else ''}."

    # Effort: rough heuristic from gap count and whether we have a builder
    slide_type = analysis.get("slide_type") or "custom"
    has_builder = slide_type in _BUILDER_DESCRIPTIONS and slide_type not in ("custom", "skip")
    if n_total == 0:
        effort_estimate = "trivial"
    elif feasibility == "fully reproducible" and has_builder:
        effort_estimate = "small"
    elif feasibility == "fully reproducible":
        effort_estimate = "medium"
    else:
        effort_estimate = "large" if len(gaps) > 3 else "medium"

    return {
        "feasibility": feasibility,
        "confidence": 100,
        "summary": summary,
        "data_needed": data_needed,
        "gaps": gaps,
        "closest_existing": slide_type if slide_type != "custom" else None,
        "effort_estimate": effort_estimate,
    }


def evaluate_new_slides(verbose: bool = False) -> list[dict[str, Any]]:
    """Scan the new-slides Drive folder; collect data-centric analysis per slide, deduce reproducibility.

    Uses the same analysis as hydrate (data_ask + purpose). Reproducibility is derived at
    report time from data_ask vs current available data keys — no separate LLM assessment.
    Results are cached so hydrate can reuse them.
    """
    global _print_context
    _print_context = "evaluate"
    folder_id = _find_new_slides_folder()
    if not folder_id:
        print(f"Create a '{NEW_SLIDES_FOLDER_NAME}' folder inside your Drive folder "
              "and drop Google Slides presentations into it.")
        return []

    presentations = _list_presentations(folder_id)
    if not presentations:
        print(f"No presentations found in '{NEW_SLIDES_FOLDER_NAME}' folder.\n"
              "Drop a Google Slides deck (with 1+ slides) into that folder and re-run.")
        return []

    _print(f"Found {len(presentations)} presentation(s) in '{NEW_SLIDES_FOLDER_NAME}':\n")
    for p in presentations:
        _print(f"  - {p['name']}")
    _print()

    slides_svc, _d, _ = _get_service()
    client = llm_client()
    all_results: list[dict[str, Any]] = []
    eval_run_hits = 0
    eval_run_slides = 0

    for pres in presentations:
        pres_id = pres["id"]
        pres_name = pres["name"]
        _print(f"{'─' * 60}")
        _print(f"Evaluating: {pres_name}")
        _print(f"{'─' * 60}\n")

        full_pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        slides = full_pres.get("slides", [])
        _print(f"  {len(slides)} slides\n")

        eval_cache = {"analysis_hit": 0, "analysis_miss": 0, "no_cache_key": 0}
        for si, slide in enumerate(slides, 1):
            page_id = slide["objectId"]
            texts = []
            for el in slide.get("pageElements", []):
                texts.extend(_extract_text(el))
            slide_text = "\n".join(texts)
            elements = _describe_elements(slide)
            title_guess = texts[0][:60] if texts else "(no text)"
            _print(f"  Slide {si}/{len(slides)}  \"{title_guess}\"")

            try:
                thumb_b64 = _get_slide_thumbnail_b64(slides_svc, pres_id, page_id)
            except Exception as e:
                logger.warning("evaluate: thumbnail unavailable for slide %d of '%s': %s",
                               si, pres_name, e)
                thumb_b64 = None

            cache_key = _slide_content_hash(thumb_b64, slide_text[:2000] if slide_text else "")
            if cache_key:
                analysis = _get_cached_slide_analysis(cache_key)
                if analysis:
                    logger.info("evaluate: [%d/%d] analysis cache hit", si, len(slides))
                    eval_cache["analysis_hit"] += 1
                else:
                    logger.info("evaluate: [%d/%d] analyzing slide (data ask + purpose)...",
                                si, len(slides))
                    analysis = _analyze_slide_broad(
                        client, slide_text, elements, thumb_b64, si, len(slides), pres_name
                    )
                    _set_cached_slide_analysis(cache_key, analysis)
                    eval_cache["analysis_miss"] += 1
            else:
                analysis = _analyze_slide_broad(
                    client, slide_text, elements, thumb_b64, si, len(slides), pres_name
                )
                eval_cache["no_cache_key"] += 1

            derived = _derive_reproducibility(analysis)
            result = {
                "presentation": pres_name,
                "slide_number": si,
                "title_guess": title_guess,
                "extracted_text": slide_text if verbose else slide_text[:200],
                "elements": elements,
                "purpose": analysis.get("purpose"),
                "slide_type": analysis.get("slide_type"),
                **derived,
            }
            all_results.append(result)
            _print_evaluation(result)

        n_ev = len(slides)
        ev_hit = eval_cache["analysis_hit"]
        logger.info(
            "evaluate: analysis cache summary for '%s' — %s | miss=%d no_key=%d",
            pres_name,
            _cache_hit_rate_line("cache_hit", ev_hit, n_ev),
            eval_cache["analysis_miss"],
            eval_cache["no_cache_key"],
        )
        _print(f"  Analysis cache: {ev_hit}/{n_ev} hits ({100 * ev_hit // n_ev if n_ev else 0}%) "
               f"(new analysis {eval_cache['analysis_miss']}, no thumbnail key {eval_cache['no_cache_key']})\n")
        eval_run_hits += ev_hit
        eval_run_slides += n_ev

    _print(f"{'=' * 60}")
    _print("CACHE HIT RATE (evaluate run — analysis cache)")
    if eval_run_slides:
        ep = 100 * eval_run_hits // eval_run_slides
        _print(f"  Analysis cache hits: {eval_run_hits}/{eval_run_slides} slides ({ep}%)")
        logger.info("evaluate: run summary — %s", _cache_hit_rate_line("analysis_cache_hit", eval_run_hits, eval_run_slides))
    else:
        _print("  No slides evaluated.")
    _print(f"{'=' * 60}")
    return all_results


def _print_evaluation(result: dict) -> None:
    """Pretty-print a single slide evaluation to stdout."""
    feasibility = result.get("feasibility", "?")
    confidence = result.get("confidence", "?")
    summary = result.get("summary", "")
    effort = result.get("effort_estimate", "?")
    closest = result.get("closest_existing")
    gaps = result.get("gaps", [])

    icon = {
        "fully reproducible": "✅",
        "mostly reproducible": "🟡",
        "partially reproducible": "🟠",
        "not reproducible": "❌",
    }.get(feasibility, "❓")

    _print(f"\n    {icon}  {feasibility}  (confidence: {confidence}%, effort: {effort})")
    _print(f"    {summary}")

    if closest:
        _print(f"    Closest existing slide: {closest}")

    data_needed = result.get("data_needed", [])
    if data_needed:
        _print("    Data:")
        for d in data_needed:
            avail = "✓" if d.get("available") else "✗"
            note = f" — {d['note']}" if d.get("note") else ""
            _print(f"      [{avail}] {d.get('source', '?')}: {d.get('fields', '?')}{note}")

    if gaps:
        _print("    Gaps:")
        for g in gaps:
            _print(f"      - {g}")

    _print()


# ── Visual QA ──

_QA_SYSTEM_PROMPT = (
    "You are a visual QA reviewer for auto-generated Google Slides presentations. "
    "Examine this slide thumbnail and identify any layout or formatting problems.\n\n"
    "Check for:\n"
    "- Text overlapping other text or elements\n"
    "- Text running off the right or bottom edge of the slide canvas\n"
    "- Text that is mid-word cut off (e.g. 'Shor' where 'Shortage' was expected) — "
    "  this is a TRUE truncation issue. Do NOT flag text that simply ends near the "
    "  right margin with a complete word — that is normal layout.\n"
    "- Unreadable font sizes (too small to read)\n"
    "- Misaligned or visually unbalanced layouts\n"
    "- Empty slides that should have content\n"
    "- Date formats that look raw/ugly (e.g. 2026-03-10 instead of March 10, 2026)\n"
    "- Color contrast issues (text invisible against background)\n"
    "- Tables with cells overflowing or misaligned\n\n"
    "IMPORTANT: Values in [brackets] like [000], [$000], [00/00/00], [00%], [???] "
    "are INTENTIONAL incomplete-data placeholders. Do NOT flag them as issues. "
    "Also do NOT flag '⚠ INCOMPLETE' banners as issues — they are intentional.\n\n"
    "Return JSON:\n"
    "  pass: boolean — true if the slide looks good, false if there are problems\n"
    "  issues: list of strings describing each problem found (empty if pass=true)\n"
    "  severity: 'none' | 'minor' | 'major' — overall severity\n"
)


def visual_qa(pres_id: str, slides_svc=None) -> list[dict[str, Any]]:
    """Thumbnail every slide in a presentation and review with GPT-4o Vision.

    Returns a list of per-slide QA results:
      {slide_num, page_id, pass, issues, severity}
    """
    if slides_svc is None:
        slides_svc, _d, _ = _get_service()

    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    slides = pres.get("slides", [])
    if not slides:
        return []

    oai = llm_client()
    n = len(slides)

    _print(f"\n  Visual QA: reviewing {n} slides...")

    # Step 1: Pre-fetch all thumbnail URLs in the main thread.
    # The Google API client (httplib2) is NOT thread-safe — calling it from workers
    # causes malloc double-free crashes.  Getting just the URL is fast (<0.5s/slide).
    logger.info("QA: fetching thumbnail URLs for %d slides...", n)
    thumb_urls: dict[str, str | None] = {}
    for si, slide in enumerate(slides, 1):
        page_id = slide["objectId"]
        try:
            thumb_urls[page_id] = _get_slide_thumbnail_url(slides_svc, pres_id, page_id)
        except Exception as e:
            logger.warning("QA: thumbnail URL failed for slide %d/%d: %s", si, n, e)
            thumb_urls[page_id] = None

    # Step 2: Parallelise the HTTP download + GPT Vision call (both thread-safe).
    def _review_slide(args: tuple[int, dict]) -> dict:
        si, slide = args
        page_id = slide["objectId"]
        url = thumb_urls.get(page_id)
        thumb_b64 = None
        if url:
            try:
                thumb_b64 = _download_thumbnail_b64(url)
            except Exception as e:
                logger.warning("QA: thumbnail download failed for slide %d/%d: %s", si, n, e)

        logger.info("QA: slide %d/%d — reviewing with %s...", si, n, LLM_MODEL)
        resp = _llm_create_with_retry(oai,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=1024,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _QA_SYSTEM_PROMPT},
                {"role": "user", "content": [
                    *(
                        [{"type": "image_url",
                          "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"}}]
                        if thumb_b64 else []
                    ),
                    {"type": "text", "text": f"Slide {si}/{n}. Review this slide."},
                ]},
            ],
        )
        raw_content = resp.choices[0].message.content
        try:
            qa = json.loads(raw_content)
        except json.JSONDecodeError as e:
            logger.warning("QA: slide %d/%d — invalid JSON from LLM (%s), treating as pass", si, n, e)
            qa = {"pass": True, "issues": ["QA response invalid (JSON error)"], "severity": "none"}
        qa["slide_num"] = si
        qa["page_id"] = page_id
        return qa

    raw: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_review_slide, (si, slide)): si
                   for si, slide in enumerate(slides, 1)}
        for fut in as_completed(futures):
            try:
                qa = fut.result()
                raw[qa["slide_num"]] = qa
            except Exception as e:
                si = futures[fut]
                logger.warning("QA failed for slide %d: %s", si, e)
                raw[si] = {"slide_num": si, "pass": True, "issues": [], "severity": "none"}

    # Emit results in slide order
    results: list[dict[str, Any]] = []
    for si in range(1, n + 1):
        qa = raw.get(si, {"slide_num": si, "pass": True, "issues": [], "severity": "none"})
        results.append(qa)
        passed = qa.get("pass", True)
        severity = qa.get("severity", "none")
        issues = qa.get("issues", [])
        if passed:
            _print(f"    [{si}/{n}] OK")
        else:
            icon = "!" if severity == "major" else "~"
            _print(f"    [{si}/{n}] [{icon}] {severity.upper()}: {'; '.join(issues[:2])}")
            for issue in issues[2:]:
                _print(f"         ↳ {issue}")

    passed_count = sum(1 for r in results if r.get("pass", True))
    failed_count = len(results) - passed_count
    major = sum(1 for r in results if r.get("severity") == "major")
    minor = sum(1 for r in results if r.get("severity") == "minor")

    _print(f"  QA result: {passed_count}/{len(results)} passed", end="")
    if failed_count:
        _print(f"  ({major} major, {minor} minor)")
    else:
        _print("")

    return results


# ── Slide data adaptation ──


def _extract_slide_text_elements(page_elements: list[dict],
                                  _depth: int = 0) -> list[dict]:
    """Extract all text and visual-data markers from slide elements.

    Handles: shapes, images, tables, sheetsCharts, and element groups (recursive).
    """
    items: list[dict] = []
    for el in page_elements:
        oid = el.get("objectId", "")

        # Pasted/imported images
        if el.get("image"):
            items.append({"type": "image", "element_id": oid, "text": "(embedded image)"})
            continue

        # Google Sheets chart — contains live numeric data, cannot be text-replaced
        if el.get("sheetsChart"):
            items.append({"type": "chart", "element_id": oid,
                          "text": "(embedded chart — contains data that cannot be auto-updated)"})
            continue

        # Element groups — recurse into children
        group = el.get("elementGroup", {})
        if group:
            children = group.get("children", [])
            items.extend(_extract_slide_text_elements(children, _depth + 1))
            continue

        shape = el.get("shape", {})

        # Shapes with image/picture fills (pasted screenshots inside a shape)
        shape_props = shape.get("shapeProperties", {})
        if shape_props.get("shapeBackgroundFill", {}).get("propertyState") == "RENDERED":
            bg_fill = shape_props.get("shapeBackgroundFill", {})
            if bg_fill.get("stretchedPictureFill"):
                items.append({"type": "image", "element_id": oid, "text": "(image in shape)"})

        # Shape text
        text_body = shape.get("text", {})
        full_text = ""
        for te in text_body.get("textElements", []):
            full_text += te.get("textRun", {}).get("content", "")
        full_text = full_text.strip()
        if full_text:
            items.append({"type": "shape", "element_id": oid, "text": full_text})

        # Tables
        table = el.get("table", {})
        if table:
            for ri, row in enumerate(table.get("tableRows", [])):
                for ci, cell in enumerate(row.get("tableCells", [])):
                    cell_text = ""
                    for te in cell.get("text", {}).get("textElements", []):
                        cell_text += te.get("textRun", {}).get("content", "")
                    cell_text = cell_text.strip()
                    if cell_text:
                        items.append({
                            "type": "table_cell", "element_id": oid,
                            "row": ri, "col": ci, "text": cell_text,
                        })
    return items


def _build_data_summary(report: dict) -> dict:
    """Compact summary of all available current data for GPT-4o matching."""
    s: dict[str, Any] = {
        "customer_name": report.get("customer", ""),
        "report_date": report.get("generated", ""),
        "quarter": report.get("quarter", ""),
        "quarter_start": report.get("quarter_start", ""),
        "quarter_end": report.get("quarter_end", ""),
    }

    acct = report.get("account", {})
    s["total_users"] = acct.get("total_visitors", 0)
    s["active_users"] = acct.get("active_visitors", 0)
    s["total_sites"] = acct.get("total_sites", 0)
    s["active_sites"] = acct.get("active_sites", 0)
    s["health_score"] = acct.get("health_score", "")

    sites = report.get("sites", [])
    s["site_details"] = [
        {
            "name": si.get("sitename", ""),
            "visitors": si.get("visitors", 0),
            "pages_used": si.get("pages_used", 0),
            "features_used": si.get("features_used", 0),
            "events": si.get("total_events", 0),
            "last_active": si.get("last_active", ""),
        }
        for si in sites[:30]
    ]

    cs = report.get("cs_platform_health", {})
    if cs:
        s["cs_health_sites"] = [
            {"site": r.get("site", ""), "health": r.get("health_status", ""),
             "ctb": r.get("ctb_pct", ""), "ctc": r.get("ctc_pct", "")}
            for r in cs.get("sites", [])[:20]
        ]

    jira = report.get("jira", {}) or report.get("jira_summary", {})
    if jira:
        s["support"] = {
            "total_tickets": jira.get("total_issues", 0),
            "open": jira.get("open_issues", 0),
            "resolved": jira.get("resolved_issues", 0),
        }

    sf = report.get("salesforce", {})
    if sf and isinstance(sf, dict) and "error" not in sf:
        s["salesforce"] = {
            "accounts": sf.get("accounts", []),
            "opportunity_count_this_year": sf.get("opportunity_count_this_year", 0),
            "pipeline_arr": sf.get("pipeline_arr", 0),
        }

    cs_val = report.get("cs_platform_value", {})
    if cs_val:
        s["platform_value"] = cs_val

    cs_sc = report.get("cs_supply_chain", {})
    if cs_sc:
        s["supply_chain"] = cs_sc

    return s


_ADAPT_SYSTEM_PROMPT = (
    "You are analyzing a slide from a customer QBR presentation. "
    "Identify every DATA VALUE on this slide — numbers, dates, percentages, "
    "currency amounts, counts, metrics — and determine whether we have "
    "current data to replace it.\n\n"
    "CURRENT DATA AVAILABLE:\n{data_json}\n\n"
    "RULES:\n"
    "- Only target DATA VALUES, never labels, headings, or descriptive text.\n"
    "- NEVER target UI elements: dropdown labels, filter values, button text, "
    "navigation text, column headers, product feature names, or any text that "
    "is part of the application interface rather than a reported metric.\n"
    "- The 'original' field must be an EXACT substring of the slide text.\n"
    "- Match by MEANING: '16 sites' maps to total_sites=14, so new_value='14 sites'.\n"
    "- Preserve the original format style: '$324k' → '$291k', '16' → '14', '03/2025' → '03/2026'.\n"
    "- For site names that appear in our data, keep them (they're still correct).\n"
    "- If a value COULD map but you're not confident, mark mapped=false.\n"
    "- Contract values, budget amounts, pricing, license costs, and any financial "
    "data not in our sources → mapped=false.\n"
    "- Specific project dates, milestones, and roadmap timelines → mapped=false.\n"
    "- HISTORICAL / RETROSPECTIVE CONTENT: If the surrounding text makes clear that a "
    "widget or bullet point is summarising past achievements, past-period results, or "
    "historical records (e.g. 'Key Partnership Results', 'What we achieved', "
    "'Since go-live', 'As of [past date]'), treat ALL values in that block as "
    "mapped=false. These are records of what happened, not live metrics to refresh.\n\n"
    "For UNMAPPED values, use these placeholders:\n"
    "  Numbers:     [000]\n"
    "  Currency:    [$000]\n"
    "  Dates:       [00/00/00]\n"
    "  Percentages: [00%]\n"
    "  Other:       [???]\n\n"
    "IMAGES & CHARTS: If the slide contains images or charts that show data:\n"
    "- Images marked '(embedded image)' or '(image in shape)': examine the thumbnail "
    "to check if the image contains data (numbers, charts, tables). If it does, add:\n"
    "  original: '(embedded image)', mapped: false,\n"
    "  new_value: '[STATIC IMAGE — contains data that cannot be auto-updated]',\n"
    "  field: brief description of what data the image shows.\n"
    "- Charts marked '(embedded chart — contains data that cannot be auto-updated)': "
    "always flag these regardless of what they show. Add:\n"
    "  original: '(embedded chart — contains data that cannot be auto-updated)', "
    "mapped: false,\n"
    "  new_value: '[CHART — data cannot be auto-updated]',\n"
    "  field: brief description of what the chart shows (e.g. 'inventory trend chart').\n\n"
    "Return JSON: {{\"replacements\": [\n"
    "  {{\"original\": \"exact text\", \"new_value\": \"replacement\", "
    "\"mapped\": true/false, \"field\": \"data source field or reason unmapped\"}}\n"
    "]}}\n"
    "Keep 'field' values short (≤10 words). "
    "Return an EMPTY replacements list if the slide has no data values to replace."
)


def _llm_create_with_retry(client, max_retries: int = 3, **kwargs):
    """Call client.chat.completions.create with exponential backoff on 429."""
    import re as _re
    from openai import NotFoundError, RateLimitError
    from .config import LLM_MODEL, LLM_PROVIDER

    delay = 30
    for attempt in range(max_retries):
        try:
            return client.chat.completions.create(**kwargs)
        except NotFoundError as e:
            logger.error(
                "LLM model not found (%s / %s). "
                "Update LLM_MODEL in src/config.py or check the provider's model list. Error: %s",
                LLM_PROVIDER, LLM_MODEL, str(e)[:200],
            )
            raise
        except RateLimitError as e:
            err_str = str(e)
            # Detect hard quota exhaustion (limit: 0) vs. transient rate limit
            hard_quota = "limit: 0" in err_str or "insufficient_quota" in err_str

            if hard_quota:
                if LLM_PROVIDER == "gemini":
                    logger.error(
                        "LLM quota exhausted (Gemini free tier). "
                        "Fix: go to console.cloud.google.com, enable billing on the project "
                        "that owns your GEMINI_API_KEY, then re-run. "
                        "Or set LLM_PROVIDER=openai in .env to use OpenAI instead."
                    )
                else:
                    logger.error(
                        "LLM quota exhausted (OpenAI). "
                        "Fix: add credits at platform.openai.com/settings/organization/billing, "
                        "or set LLM_PROVIDER=gemini in .env to use Gemini instead."
                    )
                raise  # no point retrying a hard quota error

            if attempt == max_retries - 1:
                logger.error("LLM rate limit hit %d times, giving up. Error: %s",
                             max_retries, err_str[:300])
                raise

            m = _re.search(r"retry in (\d+(?:\.\d+)?)s", err_str)
            wait = int(float(m.group(1))) + 2 if m else delay
            logger.warning("LLM rate limit — retrying in %ds (attempt %d/%d)...",
                           wait, attempt + 1, max_retries)
            time.sleep(wait)
            delay *= 2
    return None  # unreachable


def _element_may_contain_data(el: dict) -> bool:
    """Return True if this element is worth sending to GPT for data replacement.

    Filters out pure label/header text that can never be a data value, reducing
    token usage and avoiding false positives on column headers.
    """
    text = el.get("text", "")
    # Always include visual-data markers (images, charts)
    if text.startswith("(embedded") or text.startswith("(image"):
        return True
    # Skip very short labels (≤2 chars) — likely single-letter headers or bullets
    if len(text) <= 2:
        return False
    # Skip text that contains no digits and no % $ signs — pure descriptive labels
    import re as _re
    if not _re.search(r'[\d%$€£¥#]', text):
        return False
    return True


def _get_data_replacements(oai, text_elements: list[dict], data_summary: dict,
                           thumb_b64: str | None = None,
                           slide_label: str = "?") -> list[dict]:
    """Ask GPT-4o to map slide data values to current report data."""
    # Filter to elements that could plausibly contain data values
    candidates = [el for el in text_elements if _element_may_contain_data(el)]
    # Always include image/chart markers even if they slipped through the filter
    markers = [el for el in text_elements
               if el.get("text", "").startswith("(embedded") or
               el.get("text", "").startswith("(image")]
    # Merge, dedup by element_id
    seen = set()
    filtered = []
    for el in candidates + markers:
        key = (el.get("element_id"), el.get("text"))
        if key not in seen:
            seen.add(key)
            filtered.append(el)

    text_desc = "\n".join(
        f"  [{t['type']}"
        + (f" row={t['row']} col={t['col']}" if t["type"] == "table_cell" else "")
        + f"]: \"{t['text']}\""
        for t in filtered
    )

    system = _ADAPT_SYSTEM_PROMPT.format(
        data_json=json.dumps(data_summary, indent=2, default=str)[:6000]
    )

    parts: list[dict] = []
    if thumb_b64:
        parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"},
        })
    parts.append({
        "type": "text",
        "text": f"Slide text elements:\n{text_desc}\n\nIdentify all data values and map them.",
    })

    resp = _llm_create_with_retry(oai, 
        model=LLM_MODEL,
        temperature=0,
        max_tokens=4096,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": parts},
        ],
    )
    raw = resp.choices[0].message.content
    finish_reason = resp.choices[0].finish_reason
    if finish_reason == "length":
        logger.warning("hydrate: slide %s — LLM response truncated (hit max_tokens), skipping data replacement",
                       slide_label)
        return []
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("hydrate: slide %s — LLM response was invalid JSON (%s), skipping data replacement",
                       slide_label, exc)
        return []
    return result.get("replacements", [])


_PLACEHOLDER_MARKERS = ("[000]", "[$000]", "[00/00/00]", "[00%]", "[???]")
_STATIC_IMAGE_MARKER = "[STATIC IMAGE"

# Data source attribution for presenter QA: where to verify each field.
DATA_SOURCE_BY_FIELD: dict[str, str] = {
    "customer_name": "Report",
    "report_date": "Report",
    "quarter": "Report",
    "quarter_start": "Report",
    "quarter_end": "Report",
    "total_users": "Pendo",
    "active_users": "Pendo",
    "total_sites": "Pendo",
    "active_sites": "Pendo",
    "health_score": "Pendo",
    "site_details": "Pendo",
    "cs_health_sites": "CS Report",
    "support": "Jira",
    "salesforce": "Salesforce",
    "platform_value": "CS Report",
    "supply_chain": "CS Report",
}


def _build_hydrate_speaker_notes(
    replacements: list[dict],
    text_elements: list[dict],
    *,
    report: dict | None = None,
    has_unmapped: bool = False,
    has_static_images: bool = False,
    analysis: dict | None = None,
) -> str:
    """Speaker-notes for presenter QA and rebuild spec: objective, required data, governance."""
    import re as _re
    lines: list[str] = [
        "══ QA this slide — data governance ══",
        "",
        "Legend: LIVE = from our pipelines (traceable). UNMAPPED = placeholder — verify or replace. STATIC = image/chart, not auto-updated.",
        "",
    ]

    # Rebuild spec: objective and required data (when analysis available)
    if analysis:
        purpose = (analysis.get("purpose") or "").strip()
        if purpose:
            lines.append(f"Objective: {purpose}")
        title = (analysis.get("title") or "").strip()
        slide_type = (analysis.get("slide_type") or "").strip()
        if title or slide_type:
            lines.append(f"Slide: {slide_type or 'custom'}" + (f" — {title}" if title else ""))
        data_ask = analysis.get("data_ask") or []
        if data_ask:
            keys = [str(item.get("key") or item.get("field") or "?") for item in data_ask]
            lines.append("Required data: " + ", ".join(keys))
        lines.append("")

    # Data context (where/when this data is from)
    if report:
        customer = (report.get("customer") or report.get("customer_name") or "").strip()
        as_of = (report.get("generated") or report.get("report_date") or "").strip()
        quarter = (report.get("quarter") or "").strip()
        ctx_parts = [p for p in [f"Customer: {customer}" if customer else None, f"As-of: {as_of}" if as_of else None, f"Quarter: {quarter}" if quarter else None] if p]
        if ctx_parts:
            lines.append("Data context: " + " | ".join(ctx_parts))
            lines.append("")

    lines.append(f"A. Pipeline — {len(replacements)} data operation(s)")
    for i, r in enumerate(replacements, 1):
        fld = str(r.get("field") or "?")[:80]
        orig = str(r.get("original") or "").replace("\n", " ").strip()[:120]
        nv = str(r.get("new_value") or "").replace("\n", " ").strip()[:120]
        mapped = r.get("mapped", True)
        if mapped:
            source = DATA_SOURCE_BY_FIELD.get(fld, "Report/data")
            tag = f"LIVE — Source: {source}"
        else:
            tag = "UNMAPPED / static visual — verify or replace manually"
        lines.append(f"   {i}. [{fld}]  {tag}")
        lines.append(f"      was: {orig}")
        lines.append(f"      now: {nv}")
    lines.append("")
    lines.append("B. On-slide lines (numbers, $, %, or [placeholders])")
    seen: set[str] = set()
    n = 0
    for el in text_elements:
        raw = el.get("text") or ""
        typ = el.get("type", "?")
        for part in raw.split("\n"):
            s = part.strip()
            if len(s) < 2 or s in seen:
                continue
            if not _re.search(r"[\d\[\]$%€£]", s):
                continue
            seen.add(s)
            n += 1
            lines.append(f"   {n}. [{typ}] {s[:240]}")
    if n == 0:
        lines.append("   (none matched)")
    lines.append("")
    lines.append("QA checklist: ✓ Numbers match the source above? ✓ Placeholders replaced or accepted? ✓ Static images/charts current or replaced?")
    if has_unmapped or has_static_images:
        lines.append("")
        lines.append("⚠ Slide marked INCOMPLETE — contains placeholders or static images. Confirm before presenting.")
    body = "\n".join(lines)
    if len(body) > 12000:
        body = body[:11900] + "\n\n… (truncated)"
    return body


def _apply_adaptations(slides_svc, pres_id: str, page_id: str,
                       replacements: list[dict]) -> tuple[list[dict], bool, bool]:
    """Build Slides API requests to replace data values on a slide.

    Returns (requests, has_unmapped, has_static_images).
    """
    reqs: list[dict] = []
    has_unmapped = False
    has_static_images = False

    for r in replacements:
        original = r.get("original", "")
        new_value = r.get("new_value", "")
        mapped = r.get("mapped", True)

        # Static image / chart flag — can't replace pixels or chart data
        _is_visual = (
            original in ("(embedded image)", "(image in shape)",
                         "(embedded chart — contains data that cannot be auto-updated)")
            or _STATIC_IMAGE_MARKER in (new_value or "")
            or "[CHART —" in (new_value or "")
        )
        if _is_visual:
            has_static_images = True
            has_unmapped = True
            continue

        if not original or original == new_value:
            continue
        if not mapped:
            has_unmapped = True

        reqs.append({
            "replaceAllText": {
                "containsText": {"text": original, "matchCase": True},
                "replaceText": new_value,
                "pageObjectIds": [page_id],
            }
        })

    return reqs, has_unmapped, has_static_images


def _red_style_placeholders(slides_svc, pres_id: str, page_id: str) -> list[dict]:
    """Re-read a slide and return updateTextStyle requests to make placeholders red."""
    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    target_slide = None
    for sl in pres.get("slides", []):
        if sl["objectId"] == page_id:
            target_slide = sl
            break
    if not target_slide:
        return []

    red_color = {
        "foregroundColor": {
            "opaqueColor": {"rgbColor": {"red": 0.9, "green": 0.1, "blue": 0.1}}
        }
    }
    reqs: list[dict] = []

    def _scan_text_body(element_id: str, text_body: dict,
                        cell_location: dict | None = None):
        full = ""
        for te in text_body.get("textElements", []):
            full += te.get("textRun", {}).get("content", "")
        for marker in _PLACEHOLDER_MARKERS:
            start = 0
            while True:
                idx = full.find(marker, start)
                if idx == -1:
                    break
                req: dict = {
                    "updateTextStyle": {
                        "objectId": element_id,
                        "textRange": {
                            "type": "FIXED_RANGE",
                            "startIndex": idx,
                            "endIndex": idx + len(marker),
                        },
                        "style": {**red_color, "bold": True},
                        "fields": "foregroundColor,bold",
                    }
                }
                if cell_location:
                    req["updateTextStyle"]["cellLocation"] = cell_location
                reqs.append(req)
                start = idx + len(marker)

    for el in target_slide.get("pageElements", []):
        oid = el.get("objectId", "")
        shape_text = el.get("shape", {}).get("text", {})
        if shape_text:
            _scan_text_body(oid, shape_text)
        table = el.get("table", {})
        if table:
            for ri, row in enumerate(table.get("tableRows", [])):
                for ci, cell in enumerate(row.get("tableCells", [])):
                    cell_text = cell.get("text", {})
                    if cell_text:
                        _scan_text_body(oid, cell_text,
                                        cell_location={"rowIndex": ri, "columnIndex": ci})

    return reqs


def _add_incomplete_banner(page_id: str, slide_w: int = 720, slide_h: int = 405,
                           has_static_images: bool = False) -> list[dict]:
    """Create a prominent red INCOMPLETE banner across the top of a slide."""
    banner_id = f"incomplete_{page_id[:20]}"
    emu = 12700
    banner_w = slide_w - 40
    banner_h = 28
    banner_x = 20
    banner_y = 4
    if has_static_images:
        text = "INCOMPLETE — contains static image(s) with data that cannot be auto-updated"
    else:
        text = "INCOMPLETE — red values need manual update"
    reqs = [
        {
            "createShape": {
                "objectId": banner_id,
                "shapeType": "RECTANGLE",
                "elementProperties": {
                    "pageObjectId": page_id,
                    "size": {
                        "width": {"magnitude": banner_w * emu, "unit": "EMU"},
                        "height": {"magnitude": banner_h * emu, "unit": "EMU"},
                    },
                    "transform": {
                        "scaleX": 1, "scaleY": 1,
                        "translateX": banner_x * emu,
                        "translateY": banner_y * emu,
                        "unit": "EMU",
                    },
                },
            }
        },
        {
            "updateShapeProperties": {
                "objectId": banner_id,
                "shapeProperties": {
                    "shapeBackgroundFill": {
                        "solidFill": {
                            "color": {"rgbColor": {"red": 0.95, "green": 0.2, "blue": 0.2}},
                            "alpha": 0.92,
                        }
                    },
                    "outline": {"outlineFill": {"solidFill": {
                        "color": {"rgbColor": {"red": 0.8, "green": 0.1, "blue": 0.1}},
                    }}},
                },
                "fields": "shapeBackgroundFill,outline",
            }
        },
        {
            "insertText": {
                "objectId": banner_id,
                "text": text,
            }
        },
        {
            "updateTextStyle": {
                "objectId": banner_id,
                "textRange": {"type": "ALL"},
                "style": {
                    "foregroundColor": {
                        "opaqueColor": {"rgbColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}
                    },
                    "bold": True,
                    "fontSize": {"magnitude": 14, "unit": "PT"},
                },
                "fields": "foregroundColor,bold,fontSize",
            }
        },
        {
            "updateParagraphStyle": {
                "objectId": banner_id,
                "textRange": {"type": "ALL"},
                "style": {"alignment": "CENTER"},
                "fields": "alignment",
            }
        },
    ]
    return reqs


def adapt_custom_slides(slides_svc, pres_id: str, page_ids: list[str],
                        report: dict, oai) -> dict[str, Any]:
    """Adapt slides by replacing data values with current data.

    Two-phase approach:
      Phase A (parallel)  — thumbnail fetch + GPT-4o per slide (I/O bound, safe to parallelise)
      Phase B (sequential) — Slides API batchUpdate per slide (mutates shared presentation state)
    """
    data_summary = _build_data_summary(report)
    stats = {"adapted": 0, "incomplete": 0, "clean": 0, "skipped": 0}

    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    slides_by_id = {s["objectId"]: s for s in pres.get("slides", [])}
    ordered_ids = [s["objectId"] for s in pres.get("slides", [])]

    # ── Phase A: parallel GPT reasoning ──────────────────────────────────────
    # Pre-fetch all thumbnail URLs sequentially in the main thread first.
    # The Google API client (httplib2) is NOT thread-safe — calling it from
    # workers causes malloc double-free crashes on macOS.
    thumb_urls: dict[str, str | None] = {}
    for page_id in page_ids:
        slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
        logger.info("hydrate: adapt slide %s — fetching thumbnail...", slide_num)
        try:
            thumb_urls[page_id] = _get_slide_thumbnail_url(slides_svc, pres_id, page_id)
        except Exception as e:
            logger.warning("hydrate: adapt slide %s thumbnail URL failed: %s", slide_num, e)
            thumb_urls[page_id] = None

    def _fetch_and_reason(page_id: str) -> tuple[str, list[dict], list[dict], str, dict | None]:
        """Returns (page_id, text_elements, replacements, cache_source, analysis_or_none).

        cache_source: analysis_hit | adapt_hit | llm | empty | error
        analysis is included when in cache (for speaker-notes rebuild spec).
        """
        slide = slides_by_id.get(page_id)
        if not slide:
            return page_id, [], [], "empty", None
        text_elements = _extract_slide_text_elements(slide.get("pageElements", []))
        if not text_elements:
            return page_id, [], [], "empty", None
        slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"
        url = thumb_urls.get(page_id)
        thumb_b64 = None
        if url:
            try:
                thumb_b64 = _download_thumbnail_b64(url)
            except Exception:
                pass
        cache_key = _slide_content_hash(thumb_b64) if thumb_b64 else None
        analysis: dict | None = None
        if cache_key:
            analysis = _get_cached_slide_analysis(cache_key)
            if analysis and analysis.get("data_ask"):
                logger.info("hydrate: adapt slide %s — analysis cache hit (resolving data ask)",
                            slide_num)
                replacements = _resolve_data_ask_to_replacements(
                    analysis["data_ask"], data_summary, text_elements
                )
                return page_id, text_elements, replacements, "analysis_hit", analysis
            cached = _get_cached_adapt(cache_key)
            if cached is not None:
                logger.info("hydrate: adapt slide %s — adapt cache hit", slide_num)
                replacements = _resolve_cached_replacements(cached, data_summary)
                return page_id, text_elements, replacements, "adapt_hit", analysis
        n_total = len(text_elements)
        n_data = sum(1 for el in text_elements if _element_may_contain_data(el))
        logger.info("hydrate: adapt slide %s — asking %s (%d/%d elements contain data)...",
                    slide_num, LLM_MODEL, n_data, n_total)
        replacements = _get_data_replacements(oai, text_elements, data_summary, thumb_b64,
                                              slide_label=str(slide_num))
        if cache_key and replacements:
            _set_cached_adapt(cache_key, replacements)
        if cache_key and not analysis:
            analysis = _get_cached_slide_analysis(cache_key)
        return page_id, text_elements, replacements, "llm", analysis

    results: dict[str, tuple[list[dict], list[dict], dict | None]] = {}
    adapt_cache_counts: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_fetch_and_reason, pid): pid for pid in page_ids}
        for fut in as_completed(futures):
            try:
                pid, text_elements, replacements, src, analysis = fut.result()
                adapt_cache_counts[src] = adapt_cache_counts.get(src, 0) + 1
                results[pid] = (text_elements, replacements, analysis)
            except Exception as e:
                pid = futures[fut]
                sn = ordered_ids.index(pid) + 1 if pid in ordered_ids else "?"
                logger.warning("hydrate: slide %s — fetch/GPT reasoning failed: %s", sn, e)
                adapt_cache_counts["error"] = adapt_cache_counts.get("error", 0) + 1
                results[pid] = ([], [], None)

    # ── Phase B: sequential Slides API writes ──────────────────────────────────
    for page_id in page_ids:
        text_elements, replacements, analysis = results.get(page_id, ([], [], None))
        slide_num = ordered_ids.index(page_id) + 1 if page_id in ordered_ids else "?"

        if not text_elements or not replacements:
            stats["skipped"] += 1
            _print(f"    slide {slide_num}: no data values found")
            continue

        mapped_count = sum(1 for r in replacements if r.get("mapped"))
        unmapped_count = sum(1 for r in replacements if not r.get("mapped"))
        _print(f"    slide {slide_num}: {mapped_count} mapped, {unmapped_count} unmapped")

        replace_reqs, has_unmapped, has_static_images = _apply_adaptations(
            slides_svc, pres_id, page_id, replacements
        )
        if has_static_images:
            _print(f"      ↳ contains static image(s) with data")
        if replace_reqs:
            try:
                slides_svc.presentations().batchUpdate(
                    presentationId=pres_id,
                    body={"requests": replace_reqs},
                ).execute()
            except Exception as e:
                logger.warning("hydrate: slide %s — failed to apply text replacements: %s",
                               slide_num, e)
                stats["skipped"] += 1
                continue

        if has_unmapped:
            style_reqs = _red_style_placeholders(slides_svc, pres_id, page_id)
            style_reqs.extend(_add_incomplete_banner(page_id, has_static_images=has_static_images))
            try:
                slides_svc.presentations().batchUpdate(
                    presentationId=pres_id,
                    body={"requests": style_reqs},
                ).execute()
            except Exception as e:
                logger.warning("hydrate: slide %s — failed to apply red placeholder styling: %s",
                               slide_num, e)
            stats["incomplete"] += 1
        else:
            stats["clean"] += 1

        stats["adapted"] += 1
        notes = _build_hydrate_speaker_notes(
            replacements, text_elements,
            report=report,
            has_unmapped=has_unmapped,
            has_static_images=has_static_images,
            analysis=analysis,
        )
        if not set_speaker_notes(slides_svc, pres_id, page_id, notes):
            logger.warning("hydrate: slide %s — could not write speaker notes", slide_num)

    n_pages = len(page_ids)
    ah = adapt_cache_counts.get("analysis_hit", 0)
    adh = adapt_cache_counts.get("adapt_hit", 0)
    llm_n = adapt_cache_counts.get("llm", 0)
    empty_n = adapt_cache_counts.get("empty", 0)
    err_n = adapt_cache_counts.get("error", 0)
    cache_served = ah + adh
    stats["cache"] = {
        "analysis_hit": ah,
        "adapt_hit": adh,
        "llm": llm_n,
        "empty": empty_n,
        "error": err_n,
        "total_slides": n_pages,
    }
    if n_pages:
        logger.info(
            "hydrate: adapt cache summary — %s | analysis_hit=%d adapt_hit=%d llm=%d empty=%d error=%d",
            _cache_hit_rate_line("served_from_cache", cache_served, n_pages),
            ah, adh, llm_n, empty_n, err_n,
        )
    return stats


# ── Replication ──

# Slide types that use live data (vs. static/structural slides)
_DATA_SLIDE_TYPES = {
    "title", "health", "engagement", "sites", "features", "champions",
    "benchmarks", "exports", "depth", "kei", "guides", "jira", "signals",
    "platform_health", "supply_chain", "platform_value", "sla_health",
    "cross_validation", "engineering", "enhancements", "team",
    "bespoke_cover", "bespoke_deployment",
}

_STRUCTURAL_SLIDE_TYPES = {
    "bespoke_agenda", "bespoke_divider", "data_quality",
}

_BUILDER_DESCRIPTIONS = {
    "bespoke_cover": "Branded cover slide — customer name, date, 'Executive business review'",
    "bespoke_agenda": "Numbered agenda listing sections of the deck",
    "bespoke_divider": "Section divider with LeanDNA tagline and a section title",
    "bespoke_deployment": "Deployment overview — site count, health status, last active dates",
    "title": "Title slide with customer name, date range, CSM, site/user counts",
    "health": "Account health snapshot — engagement tiers, health score, benchmarks",
    "engagement": "Engagement breakdown — active/dormant counts by tier and role",
    "sites": "Site comparison table — users, pages, features, events per site",
    "features": "Feature adoption — top pages and features ranked by usage",
    "champions": "Champions & at-risk users — most active and dormant users with emails",
    "benchmarks": "Peer benchmarking — customer metrics vs cohort medians",
    "exports": "Export behavior — total exports, by feature, by user, top exporters",
    "depth": "Behavioral depth — read/write/collab breakdown by feature category",
    "kei": "Kei AI adoption — chatbot usage, adoption rate, executive engagement",
    "guides": "Guide engagement — onboarding guides seen/dismissed/advanced rates",
    "jira": "Support summary — HELP ticket counts, priority, status breakdown",
    "sla_health": "Support health & SLA — TTFR/TTR, breach rate, sentiment, channels",
    "engineering": "Engineering pipeline — LEAN project open/shipped tickets",
    "enhancements": "Enhancement requests — ER project open/shipped/declined",
    "platform_health": "Platform health — CS Report health status, CTB%, CTC%, shortages",
    "supply_chain": "Supply chain — inventory values, DOI, excess, late POs",
    "platform_value": "Platform value & ROI — savings, IA value, recs created, POs placed",
    "cross_validation": "Data cross-validation — Pendo vs CS Report engagement comparison",
    "signals": "Notable signals — auto-detected churn risk, expansion, adoption gaps",
    "team": "Team roster — CSM/AE assignments from teams.yaml",
    "data_quality": "Data quality — cross-source validation results",
    "custom": "Static content slide — reproduced text with title and body sections",
    "skip": "Skip this slide entirely (blank, transition, or not reproducible)",
}


def _classify_slide(client, text: str, elements: dict, thumb_b64: str | None,
                    slide_num: int, total: int, pres_name: str) -> dict:
    """Ask GPT-4o to classify a source slide into one of our builder types."""
    builder_list = "\n".join(f"  - {k}: {v}" for k, v in _BUILDER_DESCRIPTIONS.items())

    system = (
        "You are classifying slides from a customer-facing QBR deck. "
        "For each slide, determine which of our slide builders should be used to "
        "reproduce it with live data, or whether it should be reproduced as static text.\n\n"
        f"Available slide types:\n{builder_list}\n\n"
        "IMPORTANT classification rules:\n"
        "- Only use a data slide type if the source slide's PURPOSE clearly matches. "
        "A slide about budget, pricing, timelines, or roadmaps is ALWAYS 'custom'.\n"
        "- 'bespoke_deployment' is ONLY for slides showing a table of site names, "
        "user counts, and health status. Deployment scenarios, pricing tables, "
        "project plans, and scope descriptions are 'custom'.\n"
        "- When in doubt, choose 'custom' — it's safer to reproduce text than to "
        "map to a wrong builder that will show unrelated data.\n\n"
        "Return JSON with:\n"
        "  slide_type: one of the types above\n"
        "  title: the slide's title or section name (string)\n"
        "  reasoning: 1 sentence explaining why you chose this type\n"
        "  custom_sections: (only if slide_type='custom') [{header, body}] for text sections.\n"
        "    Keep each body SHORT — max 200 chars. Summarize rather than transcribe.\n"
        "    If the slide has tabular data, put the most important rows as a compact summary.\n"
        "    Never include more than 5 sections per slide.\n"
    )

    parts: list[dict] = []
    if thumb_b64:
        parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"},
        })
    parts.append({"type": "text", "text": (
        f"Presentation: {pres_name}\nSlide {slide_num}/{total}\n\n"
        f"Extracted text:\n{text or '(no text)'}\n\n"
        f"Elements: {json.dumps(elements)}\n\n"
        "Classify this slide."
    )})

    resp = _llm_create_with_retry(client, 
        model=LLM_MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": parts},
        ],
    )
    return json.loads(resp.choices[0].message.content)


def _detect_customer(pres_name: str, known_customers: list[str]) -> str | None:
    """Extract the customer name from a presentation title using the known customer list.
    Company/vendor names (e.g. LeanDNA) are never chosen as the customer.
    """
    name_lower = pres_name.lower()
    # Exclude our company name so e.g. "Safran & Leandna" → Safran, not Leandna
    candidates = [c for c in known_customers if c.lower() not in COMPANY_NAMES_FOR_DETECT]
    for c in sorted(candidates, key=len, reverse=True):
        if c.lower() in name_lower:
            return c
    # Fallback: ask GPT-4o-mini (our company name is never the customer)
    oai = llm_client()
    resp = _llm_create_with_retry(oai, 
        model=LLM_MODEL_FAST,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": (
                "Extract the CUSTOMER name from this presentation title (the account we serve). "
                "LeanDNA / Leandna is the vendor (our company), not the customer. "
                "In titles like 'Safran & Leandna', the customer is Safran. "
                f"Known customers: {known_customers[:80]}\n"
                "Return JSON: {\"customer\": \"<name>\" or null if not found}"
            )},
            {"role": "user", "content": pres_name},
        ],
    )
    result = json.loads(resp.choices[0].message.content)
    return result.get("customer")


def hydrate_new_slides(customer_override: str | None = None) -> list[dict[str, Any]]:
    """Hydrate presentations from new-slides/ using live data.

    Auto-detects the customer from each presentation title. If customer_override
    is provided, uses that for all decks (useful for generating a template-style
    deck for a different customer).
    """
    global _print_context
    _print_context = "hydrate"
    folder_id = _find_new_slides_folder()
    if not folder_id:
        _print(f"Create a '{NEW_SLIDES_FOLDER_NAME}' folder inside your Drive folder "
               "and drop Google Slides presentations into it.")
        return []

    presentations = _list_presentations(folder_id)
    if not presentations:
        _print(f"No presentations found in '{NEW_SLIDES_FOLDER_NAME}' folder.")
        return []

    # Load known customer names for auto-detection
    from .pendo_client import PendoClient
    from .quarters import resolve_quarter
    from .slides_client import (
        _get_service, _get_deck_output_folder, _SLIDE_BUILDERS,
    )
    from googleapiclient.errors import HttpError

    qr = resolve_quarter()
    days = qr.days
    pc = PendoClient()

    # Build customer list for auto-detection
    known_customers: list[str] = []
    if not customer_override:
        _print("Loading customer list for auto-detection...")
        try:
            known_customers = pc.get_sites_by_customer(days)["customer_list"]
        except Exception:
            pass

    slides_svc, drive_svc, _ = _get_service()
    oai = llm_client()
    all_results: list[dict[str, Any]] = []
    report_cache: dict[str, dict] = {}
    # Aggregate cache stats across all decks in this run (printed at the end)
    run_cache_totals = {
        "class_slides": 0,
        "class_avoided": 0,
        "adapt_slides": 0,
        "adapt_served": 0,
    }

    for pres in presentations:
        source_id = pres["id"]
        pres_name = pres["name"]
        _print(f"{'─' * 60}")
        _print(f"Source: {pres_name}")
        _print(f"{'─' * 60}")

        # Determine the customer for this deck
        customer = customer_override or _detect_customer(pres_name, known_customers)
        if not customer:
            _print(f"  Could not detect customer from title. "
                   f"Re-run with: decks --hydrate <customer>\n")
            all_results.append({"name": pres_name, "error": "customer not detected"})
            continue
        _print(f"  Customer: {customer}")

        # Fetch (or reuse) the health report for this customer
        if customer in report_cache:
            report = report_cache[customer]
        else:
            _print(f"  Loading data...")
            report = pc.get_customer_health_report(customer, days=days)
            if "error" in report:
                _print(f"  Failed to load data: {report['error']}\n")
                all_results.append({"name": pres_name, "customer": customer, "error": report["error"]})
                continue
            report["quarter"] = qr.label
            report["quarter_start"] = qr.start.isoformat()
            report["quarter_end"] = qr.end.isoformat()
            report_cache[customer] = report
        _print(f"  Data loaded ({days}d window, {qr.label})\n")

        full_pres = slides_svc.presentations().get(presentationId=source_id).execute()
        source_slides = full_pres.get("slides", [])
        _print(f"  {len(source_slides)} slides to classify and rebuild\n")

        class_cache = {
            "analysis_hit": 0, "legacy_classification_hit": 0,
            "fresh_analysis": 0, "no_cache_key_classify": 0,
        }
        # Phase 1: Classify every slide
        slide_plan: list[dict] = []
        for si, slide in enumerate(source_slides, 1):
            texts = []
            for el in slide.get("pageElements", []):
                texts.extend(_extract_text(el))
            slide_text = "\n".join(texts)
            elements = _describe_elements(slide)
            title_guess = texts[0][:60] if texts else "(no text)"

            logger.info("hydrate: [%d/%d] fetching thumbnail — %s",
                        si, len(source_slides), title_guess)
            try:
                thumb_b64 = _get_slide_thumbnail_b64(slides_svc, source_id, slide["objectId"])
            except Exception:
                thumb_b64 = None

            cache_key = _slide_content_hash(thumb_b64, slide_text[:2000] if slide_text else "")
            if cache_key:
                analysis = _get_cached_slide_analysis(cache_key)
                if analysis:
                    logger.info("hydrate: [%d/%d] slide analysis cache hit", si, len(source_slides))
                    class_cache["analysis_hit"] += 1
                    classification = {
                        "slide_type": analysis.get("slide_type", "custom"),
                        "title": analysis.get("title", title_guess),
                        "reasoning": analysis.get("reasoning", ""),
                        "custom_sections": analysis.get("custom_sections"),
                    }
                else:
                    classification = _get_cached_classification(cache_key)
                    if classification:
                        logger.info("hydrate: [%d/%d] classification cache hit", si, len(source_slides))
                        class_cache["legacy_classification_hit"] += 1
                    else:
                        logger.info("hydrate: [%d/%d] analyzing slide (data ask + purpose)...",
                                    si, len(source_slides))
                        classification = _analyze_slide_broad(
                            oai, slide_text, elements, thumb_b64, si, len(source_slides), pres_name
                        )
                        _set_cached_slide_analysis(cache_key, classification)
                        class_cache["fresh_analysis"] += 1
            else:
                logger.info("hydrate: [%d/%d] classifying slide (no cache key)...",
                            si, len(source_slides))
                classification = _classify_slide(
                    oai, slide_text, elements, thumb_b64, si, len(source_slides), pres_name
                )
                class_cache["no_cache_key_classify"] += 1
            slide_type = classification.get("slide_type", "custom")
            title = classification.get("title", title_guess)
            reasoning = classification.get("reasoning", "")

            _print(f"  [{si}/{len(source_slides)}] \"{title_guess}\"  → {slide_type}")
            if reasoning:
                _print(f"       {reasoning}")

            slide_plan.append({
                "slide_num": si,
                "slide_type": slide_type,
                "title": title,
                "text": slide_text,
                "elements": elements,
                "custom_sections": classification.get("custom_sections"),
            })

        n_cls = len(source_slides)
        cls_avoided = class_cache["analysis_hit"] + class_cache["legacy_classification_hit"]
        logger.info(
            "hydrate: classification cache summary — %s | analysis_hit=%d legacy_classification=%d fresh_analysis=%d no_key=%d",
            _cache_hit_rate_line("avoided_LLM", cls_avoided, n_cls),
            class_cache["analysis_hit"],
            class_cache["legacy_classification_hit"],
            class_cache["fresh_analysis"],
            class_cache["no_cache_key_classify"],
        )
        _print(f"\n  Classification cache: {cls_avoided}/{n_cls} slides avoided classification LLM "
               f"({100 * cls_avoided // n_cls if n_cls else 0}%) "
               f"[analysis={class_cache['analysis_hit']} legacy={class_cache['legacy_classification_hit']} "
               f"fresh={class_cache['fresh_analysis']} no_key={class_cache['no_cache_key_classify']}]")
        _print(f"  Classification complete. Building deck...\n")
        run_cache_totals["class_slides"] += n_cls
        run_cache_totals["class_avoided"] += cls_avoided

        # Phase 2: Copy the source presentation, then replace only data slides
        import datetime
        date_str = f"{qr.label} ({qr.start.strftime('%b %-d')} – {qr.end.strftime('%b %-d, %Y')})"
        out_title = f"{customer} — {pres_name} ({date_str})"

        try:
            copy_body: dict[str, Any] = {"name": out_title}
            output_folder = _get_deck_output_folder()
            if output_folder:
                copy_body["parents"] = [output_folder]
            copied = drive_svc.files().copy(
                fileId=source_id, body=copy_body, fields="id",
            ).execute()
            pres_id = copied["id"]
        except HttpError as e:
            status = getattr(e.resp, "status", None) or "?"
            reason = getattr(e.resp, "reason", "") or ""
            detail = ""
            try:
                body = json.loads(e.content.decode("utf-8")) if getattr(e, "content", None) else {}
                err = body.get("error", {})
                if isinstance(err, dict):
                    detail = err.get("message", "")
                    errs = err.get("errors", [])
                    if errs and isinstance(errs[0], dict) and errs[0].get("reason"):
                        detail = f"{detail} ({errs[0]['reason']})" if detail else errs[0]["reason"]
            except Exception:
                pass
            _print(f"  FAIL copying presentation: HTTP {status} {reason}")
            if detail:
                _print(f"    {detail}")
            _print(f"    Source file (in '{NEW_SLIDES_FOLDER_NAME}' folder): {pres_name}")
            _print(f"    File ID: {source_id}")
            _print(f"    Open: https://docs.google.com/presentation/d/{source_id}/edit")
            if status == 403:
                _print("    Hint: Service account may lack access to the source file or output folder.")
            elif status == 404:
                _print("    Hint: The service account can read this file but cannot copy it.")
                _print("    Fix: Open the source file in Drive, click Share, and give the service account 'Editor' access.")
            all_results.append({
                "name": pres_name,
                "error": f"copy failed: HTTP {status} {detail or str(e)}",
                "source_id": source_id,
                "source_url": f"https://docs.google.com/presentation/d/{source_id}/edit",
            })
            continue

        # Read the slide object IDs from the copy
        copy_pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        copy_slides = copy_pres.get("slides", [])

        # Attach slide plan to report so builders like bespoke_agenda can use it
        report["_slide_plan"] = [
            {"id": sp["slide_type"], "slide_type": sp["slide_type"], "title": sp["title"]}
            for sp in slide_plan if sp["slide_type"] != "skip"
        ]
        # Phase 2: Delete slides classified as "skip", and rebuild the one purely-mechanical
        # slide type (data_quality — color-coded boxes, no editorial content).
        # All other slides are kept exactly as-is; their data values will be updated
        # in-place during Phase 3.  Builder functions belong to the "build" path and
        # must NOT be called here, to avoid overwriting the customer's editorial content.
        reqs: list[dict] = []
        delete_ids: list[str] = []
        offset = 0
        built = 0
        kept = 0
        skipped = 0

        for i, sp in enumerate(slide_plan):
            st = sp["slide_type"]
            orig_oid = copy_slides[i]["objectId"] if i < len(copy_slides) else None

            if st == "skip":
                if orig_oid:
                    delete_ids.append(orig_oid)
                skipped += 1
                continue

            # data_quality is 100% mechanical (colored indicators, no editorial text);
            # rebuild it so current source health is always accurate.
            if st == "data_quality":
                builder = _SLIDE_BUILDERS.get("data_quality")
                if builder and orig_oid:
                    report["_current_slide"] = {"id": st, "slide_type": st, "title": sp["title"]}
                    insert_idx = i + 1 + offset
                    sid = f"s_dq_{i}"
                    try:
                        new_idx = builder(reqs, sid, report, insert_idx)
                        created = new_idx - insert_idx
                        if created > 0:
                            offset += created
                            delete_ids.append(orig_oid)
                            built += 1
                            continue
                    except Exception as e:
                        logger.warning("data_quality builder failed, keeping original: %s", e)
                kept += 1
                continue

            kept += 1

        # Delete the original slides that we replaced
        for oid in delete_ids:
            reqs.append({"deleteObject": {"objectId": oid}})

        # Execute
        url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
        logger.info("hydrate: phase 2 — built=%d rebuilt, kept=%d, skipped=%d",
                    built, kept, skipped)
        if reqs:
            try:
                slides_svc.presentations().batchUpdate(
                    presentationId=pres_id, body={"requests": reqs},
                ).execute()
            except HttpError as e:
                _print(f"  FAIL applying slide changes: {e}")
                all_results.append({"name": pres_name, "error": str(e)[:200]})
                continue

        _print(f"  Phase 2: {built} rebuilt, {kept} kept, {skipped} skipped")
        _print(f"  Output: {url}")

        # Phase 3: Adapt ALL kept slides — replace data values in-place, mark unmapped
        # fields with red placeholders.  This covers every slide regardless of type;
        # the classification in Phase 1 is for reporting only, not for gating adaptation.
        # Re-fetch slide list after Phase 2 mutations so objectIds are current.
        adapted_pres = slides_svc.presentations().get(presentationId=pres_id).execute()
        adapted_slides = adapted_pres.get("slides", [])
        adapt_page_ids = [s["objectId"] for s in adapted_slides]

        if adapt_page_ids:
            _print(f"\n  Adapting {len(adapt_page_ids)} slides with current data...")
            adapt_stats = adapt_custom_slides(
                slides_svc, pres_id, adapt_page_ids, report, oai,
            )
            _print(f"  Adapted: {adapt_stats['adapted']} slides "
                   f"({adapt_stats['clean']} clean, {adapt_stats['incomplete']} incomplete, "
                   f"{adapt_stats['skipped']} unchanged)")
            c = adapt_stats.get("cache") or {}
            tot = c.get("total_slides") or 0
            if tot:
                served = c.get("analysis_hit", 0) + c.get("adapt_hit", 0)
                _print(f"  Adapt cache: {served}/{tot} slides avoided adapt LLM "
                       f"({100 * served // tot if tot else 0}%) "
                       f"[analysis={c.get('analysis_hit', 0)} legacy_adapt={c.get('adapt_hit', 0)} "
                       f"llm={c.get('llm', 0)}]")
                run_cache_totals["adapt_slides"] += tot
                run_cache_totals["adapt_served"] += served

        # Visual QA — review every slide for layout problems
        logger.info("hydrate: running visual QA on '%s'...", pres_name)
        qa_results = visual_qa(pres_id, slides_svc=slides_svc)
        qa_issues = [r for r in qa_results if not r.get("pass", True)]
        logger.info("hydrate: QA complete — %d passed, %d issues",
                    len(qa_results) - len(qa_issues), len(qa_issues))

        result_entry: dict[str, Any] = {
            "name": pres_name, "customer": customer,
            "url": url, "built": built, "kept": kept, "skipped": skipped,
            "qa_passed": len(qa_results) - len(qa_issues),
            "qa_failed": len(qa_issues),
        }
        if qa_issues:
            result_entry["qa_issues"] = [
                {"slide": r["slide_num"], "severity": r.get("severity"),
                 "issues": r.get("issues", [])}
                for r in qa_issues
            ]
        all_results.append(result_entry)
        _print("")

    _print(f"{'=' * 60}")
    _print(f"Replication complete: {len(all_results)} deck(s)")
    _print(f"{'=' * 60}")
    _print("")
    _print("CACHE HIT RATE (entire run)")
    cs, ca = run_cache_totals["class_slides"], run_cache_totals["class_avoided"]
    if cs:
        cp = 100 * ca // cs
        _print(f"  Classification LLM skipped: {ca}/{cs} slides ({cp}% hit rate)")
        logger.info("hydrate: run summary — classification cache %s", _cache_hit_rate_line("hit", ca, cs))
    else:
        _print("  Classification: no slides processed this run")
    ads, adh = run_cache_totals["adapt_slides"], run_cache_totals["adapt_served"]
    if ads:
        ap = 100 * adh // ads
        _print(f"  Adapt LLM skipped:       {adh}/{ads} slides ({ap}% hit rate)")
        logger.info("hydrate: run summary — adapt cache %s", _cache_hit_rate_line("hit", adh, ads))
    else:
        _print("  Adapt: no adapt phase ran this run (or all decks failed before adapt)")
    _print(f"{'=' * 60}")
    return all_results
