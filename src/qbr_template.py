"""QBR deck generator: copy Drive template, manifest-driven hide/reorder, optional exec summary, adapt."""

from __future__ import annotations

import copy
import datetime
import hashlib
import json
import time
from typing import Any

from googleapiclient.errors import HttpError

from .config import (
    BPO_SIGNALS_LLM,
    BPO_SIGNALS_LLM_DECK_PROMPT,
    BPO_SIGNALS_LLM_EDITORIAL,
    BPO_SIGNALS_LLM_MANIFEST_MAX_CHARS,
    GOOGLE_QBR_GENERATOR_FOLDER_ID,
    GOOGLE_QBR_OUTPUT_PARENT_ID,
    GOOGLE_QBR_TEMPLATE_FOLDER_ID,
    LLM_MODEL,
    QBR_TEMPLATE_FILE_NAME,
    logger,
    llm_client,
)
from .deck_loader import resolve_deck
from .slide_loader import hydrate_hints_by_slide_id
from .drive_config import (
    ADAPT_SYSTEM_PROMPT_FILENAME,
    export_google_doc_as_plain_text,
    find_file_in_folder,
    get_qbr_generator_folder_id_for_drive_config,
    _find_or_create_folder,
)
from .evaluate import _detect_customer, _extract_text, adapt_custom_slides
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence
from .pendo_client import PendoClient
from .pendo_portfolio_snapshot_drive import (
    ensure_daily_portfolio_snapshot_for_qbr,
    portfolio_snapshot_filename,
    try_load_portfolio_snapshot_for_request,
)
from .qbr_adapt_hints import (
    apply_qbr_template_style_strip_after_adapt,
    run_qbr_adapt_hints_phase,
)
from .qbr_agenda_visual_refine import (
    find_qbr_agenda_page_id,
    run_qbr_agenda_visual_refinement_loop,
)
from .signals_llm import extract_executive_signals_slide_prompt
from .quarters import QuarterRange, resolve_quarter
from .slides_client import (
    _build_slide_jql_speaker_notes,
    _get_service,
    _google_api_unreachable_hint,
    _normalize_builder_return,
    _SLIDE_BUILDERS,
    apply_cohort_bundle_links_to_notable_signals,
    create_cohort_deck,
    create_health_deck,
    presentations_batch_update_chunked,
    set_speaker_notes,
    slides_presentations_batch_update,
)

# Drive layout under the QBR Generator folder (see get_qbr_generator_folder_id_for_drive_config)
QBR_OUTPUT_SUBFOLDER = "Output"
# qbr_slide_list (Google Doc) + (via drive_config) repo ``prompts/adapt_system_prompt.yaml`` synced here for adapt
QBR_PROMPTS_SUBFOLDER = "Prompts"
QBR_SLIDE_LIST_DOC_NAME = "qbr_slide_list"

_MIME_PRESENTATION = "application/vnd.google-apps.presentation"
_MIME_FOLDER = "application/vnd.google-apps.folder"
_MIME_DOC = "application/vnd.google-apps.document"


def _slide_title_text(slide: dict) -> str:
    texts: list[str] = []
    for el in slide.get("pageElements", []):
        texts.extend(_extract_text(el))
    joined = " ".join(texts).strip()
    return (joined[:500] if joined else "(no text)") or "(no text)"


def build_slide_inventory(slides: list[dict]) -> list[dict[str, Any]]:
    """1-based index, title guess, objectId for each slide (template copy)."""
    out: list[dict[str, Any]] = []
    for i, s in enumerate(slides):
        out.append({
            "index": i + 1,
            "title": _slide_title_text(s),
            "objectId": s["objectId"],
        })
    return out


def resolve_qbr_template_and_manifest(generator_folder_id: str) -> tuple[str, str]:
    """Return (template_presentation_file_id, manifest_plain_text).

    Template Slides file: ``QBR_TEMPLATE_FILE_NAME`` (from env) under ``GOOGLE_QBR_TEMPLATE_FOLDER_ID`` if set,
    else under ``generator_folder_id``. Prompts / ``qbr_slide_list`` always use ``generator_folder_id``.
    """
    template_folder = GOOGLE_QBR_TEMPLATE_FOLDER_ID or generator_folder_id
    tid = find_file_in_folder(
        QBR_TEMPLATE_FILE_NAME, template_folder, _MIME_PRESENTATION
    )
    if not tid:
        raise FileNotFoundError(
            f"QBR template not found: '{QBR_TEMPLATE_FILE_NAME}' under folder {template_folder}"
        )
    prompts_id = find_file_in_folder(QBR_PROMPTS_SUBFOLDER, generator_folder_id, _MIME_FOLDER)
    if not prompts_id:
        raise FileNotFoundError(
            f"QBR Prompts folder not found: '{QBR_PROMPTS_SUBFOLDER}' under {generator_folder_id}"
        )
    if not find_file_in_folder(ADAPT_SYSTEM_PROMPT_FILENAME, prompts_id, None):
        raise FileNotFoundError(
            f"{ADAPT_SYSTEM_PROMPT_FILENAME} not found under Prompts folder (id={prompts_id})"
        )
    mid = find_file_in_folder(QBR_SLIDE_LIST_DOC_NAME, prompts_id, _MIME_DOC)
    if not mid:
        raise FileNotFoundError(
            f"qbr_slide_list Google Doc not found: '{QBR_SLIDE_LIST_DOC_NAME}' under Prompts folder"
        )
    text = export_google_doc_as_plain_text(mid)
    return tid, text


def get_qbr_output_folder_id() -> str | None:
    """Return folder id for ``{ISO-date} - Output``, creating it if needed.

    Default parent chain: ``<QBR Generator>/Output/``. Override with ``GOOGLE_QBR_OUTPUT_PARENT_ID``
    (parent of the date-stamped folder only).
    """
    if GOOGLE_QBR_OUTPUT_PARENT_ID:
        parent = GOOGLE_QBR_OUTPUT_PARENT_ID
    else:
        if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
            logger.warning(
                "QBR: GOOGLE_QBR_GENERATOR_FOLDER_ID not set — cannot create Output folder under generator"
            )
            return None
        gen = get_qbr_generator_folder_id_for_drive_config()
        parent = _find_or_create_folder(QBR_OUTPUT_SUBFOLDER, gen)
    name = f"{datetime.date.today().isoformat()} - Output"
    return _find_or_create_folder(name, parent)


def _drive_safe_folder_fragment(s: str, max_len: int = 120) -> str:
    """Strip characters Google Drive disallows in folder names."""
    bad = '/\\:?*"|<>#[]'
    t = "".join("-" if c in bad else c for c in (s or ""))
    t = t.strip()[:max_len].rstrip("-. ")
    return t or "customer"


def ensure_qbr_customer_bundle_folder(
    output_folder_id: str,
    customer: str,
    quarter_label: str,
) -> str:
    """Create ``{customer} — QBR bundle ({quarter})`` under the date Output folder."""
    c = _drive_safe_folder_fragment(customer)
    q = _drive_safe_folder_fragment(quarter_label, max_len=48)
    name = f"{c} — QBR bundle ({q})"
    return _find_or_create_folder(name, output_folder_id)


# (deck_id, result key) — standalone decks placed next to the hydrated QBR file for CSM context.
QBR_BUNDLE_COMPANION_DECKS: tuple[tuple[str, str], ...] = (
    ("cs_health_review", "health_review"),
    ("executive_summary", "executive_summary"),
    ("support", "support"),
    ("product_adoption", "product_adoption"),
    ("cohort_review", "cohort_review"),
)


def _quarter_range_from_health_report(report: dict[str, Any]) -> QuarterRange | None:
    """Rebuild ``QuarterRange`` from fields set on QBR / health reports (for cohort portfolio window)."""
    from datetime import date

    label = report.get("quarter")
    qs = report.get("quarter_start")
    qe = report.get("quarter_end")
    if not label or not qs or not qe:
        return None
    try:
        start = date.fromisoformat(str(qs)[:10])
        end = date.fromisoformat(str(qe)[:10])
        return QuarterRange(label=str(label), start=start, end=end)
    except ValueError:
        return None


def _build_companion_decks_for_qbr_bundle(
    report: dict[str, Any],
    bundle_folder_id: str,
) -> list[dict[str, Any]]:
    """Generate health, exec summary, support, product adoption, and cohort review decks into the bundle folder.

    When a fresh JSON portfolio snapshot exists on Drive (folder from
    ``resolve_portfolio_snapshot_folder_id`` in ``pendo_portfolio_snapshot_drive``),
    cohort_review uses it and no background Pendo
    thread runs. Otherwise the portfolio report is computed in a background thread
    while the first four companion decks build so the two phases overlap.
    """
    import threading

    base = copy.deepcopy(report)
    for k in ("_slide_plan", "_current_slide", "_charts", "_slides_svc", "_drive_svc"):
        base.pop(k, None)

    days = int(base.get("days") or 30)
    qr = _quarter_range_from_health_report(base)

    portfolio_result: dict[str, Any] = {}
    portfolio_error: BaseException | None = None
    portfolio_thread: threading.Thread | None = None

    snap = try_load_portfolio_snapshot_for_request(days, None)
    if snap is not None:
        portfolio_result = snap
        logger.info(
            "QBR bundle: cohort_review will use Drive portfolio snapshot (%d customers); "
            "skipping background Pendo precompute",
            portfolio_result.get("customer_count", 0),
        )
    else:
        logger.info(
            "QBR bundle: no usable portfolio snapshot — cohort waits on live Pendo after "
            "four companion decks (they usually dominate wall time; snapshot skips work that "
            "overlapped with them). Expected Drive file: %s",
            portfolio_snapshot_filename(days, None),
        )

        def _precompute_portfolio() -> None:
            nonlocal portfolio_result, portfolio_error
            try:
                logger.info(
                    "QBR bundle: starting portfolio precompute for cohort_review "
                    "(runs in background while other companions build)"
                )
                pc = PendoClient()
                portfolio_result = pc.get_portfolio_report(days=days)
            except BaseException as e:
                portfolio_error = e
                logger.warning("QBR bundle: portfolio precompute failed: %s", e)

        portfolio_thread = threading.Thread(target=_precompute_portfolio, daemon=True)
        portfolio_thread.start()

    out: list[dict[str, Any]] = []
    t_bundle0 = time.perf_counter()
    for deck_id, key in QBR_BUNDLE_COMPANION_DECKS:
        sub = copy.deepcopy(base)
        t_deck0 = time.perf_counter()
        try:
            if deck_id == "cohort_review":
                if portfolio_thread is not None:
                    portfolio_thread.join()
                if portfolio_error:
                    raise portfolio_error
                cr = create_cohort_deck(
                    days=days,
                    max_customers=None,
                    quarter=qr,
                    thumbnails=False,
                    output_folder_id=bundle_folder_id,
                    portfolio_report=portfolio_result,
                )
            else:
                cr = create_health_deck(
                    sub,
                    deck_id=deck_id,
                    thumbnails=False,
                    output_folder_id=bundle_folder_id,
                )
            entry: dict[str, Any] = {
                "key": key,
                "deck_id": deck_id,
                "presentation_id": cr.get("presentation_id"),
                "url": cr.get("url"),
            }
            if cr.get("error"):
                entry["error"] = cr["error"]
                if cr.get("hint"):
                    entry["hint"] = cr["hint"]
                logger.warning("QBR bundle companion %s failed: %s", deck_id, cr["error"])
            else:
                logger.info("QBR bundle companion %s → %s", deck_id, cr.get("url", ""))
            out.append(entry)
        except Exception as e:
            logger.warning("QBR bundle companion %s failed: %s", deck_id, e)
            row = {"key": key, "deck_id": deck_id, "error": str(e)[:500]}
            gh = _google_api_unreachable_hint(e)
            if gh:
                row["hint"] = gh
            out.append(row)
        finally:
            logger.info(
                "QBR bundle companion %s wall time %.1fs (running total %.1fs)",
                deck_id,
                time.perf_counter() - t_deck0,
                time.perf_counter() - t_bundle0,
            )
    return out


def _normalize_manifest_plan(raw: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    hide = raw.get("hide") or {}
    if not isinstance(hide, dict):
        hide = {}
    tc = hide.get("title_contains") or []
    if not isinstance(tc, list):
        tc = []
    idxs = hide.get("indices") or []
    if not isinstance(idxs, list):
        idxs = []
    coerced_idx: list[int] = []
    for x in idxs:
        if isinstance(x, bool):
            continue
        if isinstance(x, int):
            coerced_idx.append(x)
        elif isinstance(x, float) and x == int(x):
            coerced_idx.append(int(x))
        else:
            s = str(x).strip()
            if s.isdigit():
                coerced_idx.append(int(s))
    move = raw.get("move_to_end_title_contains") or []
    if not isinstance(move, list):
        move = []
    notes = raw.get("notes", "")
    if not isinstance(notes, str):
        notes = str(notes)
    ins = raw.get("insert_executive_summary", False)
    if isinstance(ins, str):
        insert_es = ins.strip().lower() in ("true", "1", "yes")
    else:
        insert_es = bool(ins)
    return {
        "insert_executive_summary": insert_es,
        "hide": {
            "title_contains": [str(x).strip() for x in tc if str(x).strip()],
            "indices": coerced_idx,
        },
        "move_to_end_title_contains": [str(x).strip() for x in move if str(x).strip()],
        "notes": notes.strip(),
    }


_MANIFEST_PLANNER_SYSTEM = """You configure a Google Slides QBR deck from a template.

You receive:
1) MANIFEST_RULES — organizational rules from the customer/org (read carefully: they govern hide, reorder, AND whether to insert an executive-summary block).
2) CUSTOMER — the account name for this run (use for interpreting segment rules).
3) SLIDES — JSON array of slides as copied from the template (no inserts yet). Each item: index (1-based), title (extracted text), objectId (opaque id).

Return ONLY valid JSON with this exact shape:
{
  "insert_executive_summary": true or false,
  "hide": {
    "title_contains": ["substring to match slide title", ...],
    "indices": [3, 4]
  },
  "move_to_end_title_contains": ["substring for slides to move to end", ...],
  "notes": "one-line summary of what you applied"
}

Rules:
- insert_executive_summary: set true ONLY if MANIFEST_RULES say to add an executive-summary section (e.g. after the title slide). If the manifest says not to, or is silent, set false.
- "indices" refer to 1-based positions in SLIDES as given (template as copied).
- Prefer title_contains when the manifest names a slide by topic; use indices when the manifest uses explicit slide numbers.
- Do not list index 1 in "indices" for hiding unless the manifest explicitly requires hiding the opening/title slide.
- move_to_end_title_contains: slides whose title contains the substring (case-insensitive) are moved to the end of the deck after other steps; only template-origin slides will be moved (the tool filters).
- If the manifest does not require hiding or moving, return empty arrays.
"""


def call_manifest_planner(
    oai: Any,
    manifest_text: str,
    customer: str,
    inventory: list[dict[str, Any]],
) -> dict[str, Any]:
    user_payload = {
        "MANIFEST_RULES": manifest_text[:12000],
        "CUSTOMER": customer,
        "SLIDES": inventory,
    }
    resp = _llm_create_with_retry(
        oai,
        model=LLM_MODEL,
        temperature=0,
        max_tokens=2048,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": _MANIFEST_PLANNER_SYSTEM},
            {"role": "user", "content": json.dumps(user_payload, indent=0, default=str)},
        ],
    )
    raw = _strip_json_code_fence(resp.choices[0].message.content or "")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("QBR manifest planner returned invalid JSON: %s", e)
        return _normalize_manifest_plan({})
    return _normalize_manifest_plan(data)


def resolve_hide_object_ids(
    plan: dict[str, Any],
    inventory: list[dict[str, Any]],
) -> set[str]:
    """Map hide rules to template objectIds. Title slide (index 1) is never hidden."""
    hide_ids: set[str] = set()
    by_index = {int(item["index"]): item["objectId"] for item in inventory if "index" in item}
    for idx in plan.get("hide", {}).get("indices", []):
        try:
            i = int(idx)
        except (TypeError, ValueError):
            continue
        oid = by_index.get(i)
        if oid:
            hide_ids.add(oid)
    for sub in plan.get("hide", {}).get("title_contains", []):
        sub_l = sub.lower()
        for item in inventory:
            if sub_l in (item.get("title") or "").lower():
                hide_ids.add(item["objectId"])
    if inventory:
        hide_ids.discard(inventory[0]["objectId"])
    return hide_ids


def _apply_slide_skipped(
    slides_svc: Any,
    pres_id: str,
    object_ids: set[str],
) -> None:
    if not object_ids:
        return
    reqs = [
        {
            "updateSlideProperties": {
                "objectId": oid,
                "slideProperties": {"isSkipped": True},
                "fields": "isSkipped",
            }
        }
        for oid in object_ids
    ]
    slides_presentations_batch_update(slides_svc, pres_id, reqs)


def _apply_move_template_slides_to_end(
    slides_svc: Any,
    pres_id: str,
    title_substrings: list[str],
    template_object_ids: frozenset[str],
) -> None:
    if not title_substrings:
        return
    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    slides = pres.get("slides", [])
    id_order = [s["objectId"] for s in slides]
    slide_by_id = {s["objectId"]: s for s in slides}

    def title_of(oid: str) -> str:
        return _slide_title_text(slide_by_id.get(oid, {}))

    to_move: list[str] = []
    for oid in id_order:
        if oid not in template_object_ids:
            continue
        t = title_of(oid)
        tl = t.lower()
        for sub in title_substrings:
            if sub.lower() in tl:
                to_move.append(oid)
                break
    if not to_move:
        return
    rest = [x for x in id_order if x not in to_move]
    insertion_index = len(rest)
    slides_presentations_batch_update(
        slides_svc,
        pres_id,
        [{
            "updateSlidesPosition": {
                "slideObjectIds": to_move,
                "insertionIndex": insertion_index,
            },
        }],
    )


def _insert_executive_summary_slides(
    slides_svc: Any,
    pres_id: str,
    report: dict[str, Any],
    customer: str,
) -> tuple[list[str], int, list[str]]:
    """Insert resolved executive_summary deck after template slide 1.

    Returns ``(object_ids, manifest_slide_count, signals_page_ids)`` where
    ``manifest_slide_count`` is how many deck entries had a builder (one per YAML slide row);
    ``len(object_ids)`` may be larger when a slide type paginates into multiple pages.
    ``signals_page_ids`` lists physical slide object IDs for ``slide_type == "signals"`` (for
    post-linking to the cohort review deck in the QBR bundle).
    """
    resolved = resolve_deck("executive_summary", customer)
    if resolved.get("error"):
        raise RuntimeError(resolved["error"])
    slide_plan = resolved.get("slides") or []
    if not slide_plan:
        raise RuntimeError("executive_summary deck has no slides")

    reqs: list[dict] = []
    idx = 1
    exec_ids: list[str] = []
    signals_page_ids: list[str] = []
    manifest_built = 0
    note_targets: list[tuple[str, dict[str, Any]]] = []
    report = dict(report)
    report["_slide_plan"] = slide_plan

    for entry in slide_plan:
        slide_type = entry.get("slide_type", entry["id"])
        builder = _SLIDE_BUILDERS.get(slide_type)
        if not builder:
            logger.warning("QBR: no builder for slide_type=%s, skipping", slide_type)
            continue
        manifest_built += 1
        report["_current_slide"] = entry
        sid = f"qbr_es_{entry['id']}_{idx}"
        ret = builder(reqs, sid, report, idx)
        next_idx, page_ids = _normalize_builder_return(ret, sid)
        for nid in page_ids:
            exec_ids.append(nid)
            note_targets.append((nid, dict(entry)))
        if slide_type == "signals":
            signals_page_ids.extend(page_ids)
        idx = next_idx

    if not reqs:
        raise RuntimeError("executive_summary: no slide requests generated")

    presentations_batch_update_chunked(slides_svc, pres_id, reqs)

    for sid, entry in note_targets:
        notes = _build_slide_jql_speaker_notes(report, entry)
        if not set_speaker_notes(slides_svc, pres_id, sid, notes):
            logger.warning("QBR: could not set speaker notes on %s", sid[:16])

    try:
        _apply_slide_skipped(slides_svc, pres_id, set(exec_ids))
        logger.info("QBR: marked %d executive summary slide(s) as skipped for slideshow", len(exec_ids))
    except HttpError as e:
        logger.warning("QBR: could not mark executive summary slides skipped: %s", e)

    return exec_ids, manifest_built, signals_page_ids


def compute_adapt_page_ids(
    final_slides_ordered: list[dict],
    title_slide_object_id: str,
    exec_slide_object_ids: frozenset[str],
) -> list[str]:
    """In-place adapt: all slides except template title and (if any) inserted exec-summary slides. Hidden slides included."""
    out: list[str] = []
    for s in final_slides_ordered:
        oid = s["objectId"]
        if oid == title_slide_object_id:
            continue
        if oid in exec_slide_object_ids:
            continue
        out.append(oid)
    return out


def run_qbr_from_template(customer_query: str) -> dict[str, Any]:
    """End-to-end QBR: copy template, manifest plan (optional exec-summary insert), hide/move, adapt.

    Creates ``<QBR Generator>/Output/{date} - Output/{customer} — QBR bundle ({quarter})/`` (unless
    ``GOOGLE_QBR_OUTPUT_PARENT_ID`` overrides the parent of ``{date} - Output``), and places the hydrated QBR
    deck there together with standalone Customer Success Health Review, Executive Summary,
    Support Review, Product Adoption Review, and Manufacturing Cohort Review decks
    (cohort deck uses the same quarter window and a full portfolio rollup; other companions
    use the single-customer health report). After preload, may auto-build/upload the Drive
    portfolio snapshot once per calendar day (see ``ensure_daily_portfolio_snapshot_for_qbr``).

    Returns a result dict with ``url``, ``bundle_folder_id``, ``companion_decks``, and logging fields.
    """
    try:
        gen_id = get_qbr_generator_folder_id_for_drive_config()
    except RuntimeError as e:
        return {"error": str(e), "hint": "Set GOOGLE_QBR_GENERATOR_FOLDER_ID in .env to your QBR Generator folder id."}

    logger.info(
        "QBR: generator base folder id=%s — https://drive.google.com/drive/folders/%s",
        gen_id,
        gen_id,
    )

    try:
        template_id, manifest_text = resolve_qbr_template_and_manifest(gen_id)
    except FileNotFoundError as e:
        return {"error": str(e)}

    mf_hash = hashlib.sha256(manifest_text.encode("utf-8", errors="replace")).hexdigest()[:16]
    logger.info("QBR: manifest loaded (%d chars, sha256[:16]=%s)", len(manifest_text), mf_hash)

    qr = resolve_quarter()
    days = qr.days
    pc = PendoClient()
    try:
        pc.preload(days)
        partition = pc._get_visitor_partition(days)
        known = sorted(c for c in partition.get("all_customer_stats", {}) if c != "?")
    except Exception as e:
        return {"error": f"Pendo preload / customer list failed: {e}"}

    ensure_daily_portfolio_snapshot_for_qbr(days, None)

    customer = _detect_customer(customer_query, known)
    if not customer:
        return {
            "error": f"Could not resolve customer from {customer_query!r}",
            "hint": "Use a substring that matches a Pendo customer name",
        }

    mf_excerpt: str | None = None
    sig_prompt: str | None = None
    if BPO_SIGNALS_LLM and BPO_SIGNALS_LLM_EDITORIAL:
        if manifest_text and manifest_text.strip():
            mf_excerpt = manifest_text.strip()[:BPO_SIGNALS_LLM_MANIFEST_MAX_CHARS]
        if BPO_SIGNALS_LLM_DECK_PROMPT:
            sig_prompt = extract_executive_signals_slide_prompt(customer)

    report = pc.get_customer_health_report(
        customer,
        days=days,
        signals_llm_manifest_rules=mf_excerpt,
        signals_llm_slide_prompt=sig_prompt,
    )
    if "error" in report:
        return {"error": report["error"], "customer_query": customer_query}

    report["quarter"] = qr.label
    report["quarter_start"] = qr.start.isoformat()
    report["quarter_end"] = qr.end.isoformat()

    # Enrich with LeanDNA Item Master Data if configured
    try:
        from .leandna_item_master_enrich import enrich_qbr_with_item_master
        report = enrich_qbr_with_item_master(report, customer)
    except Exception as e:
        logger.warning("LeanDNA Item Master enrichment failed (non-fatal): %s", e)
    
    # Enrich with LeanDNA Material Shortage Trends if configured
    try:
        from .leandna_shortage_enrich import enrich_qbr_with_shortage_trends
        report = enrich_qbr_with_shortage_trends(report, customer, weeks_forward=12)
    except Exception as e:
        logger.warning("LeanDNA Shortage Trends enrichment failed (non-fatal): %s", e)

    qbr_resolved = resolve_deck("qbr", customer)
    if qbr_resolved.get("error"):
        logger.warning(
            "QBR: resolve_deck('qbr') failed — on-slide agenda titles will not hydrate: %s",
            qbr_resolved.get("error"),
        )
        report["_slide_plan"] = []
    else:
        report["_slide_plan"] = qbr_resolved.get("slides") or []

    report["_hydrate_slide_hints"] = hydrate_hints_by_slide_id()

    slides_svc, drive_svc, _google_creds = _get_service()
    output_folder = get_qbr_output_folder_id()
    bundle_folder_id: str | None = None
    if output_folder:
        try:
            bundle_folder_id = ensure_qbr_customer_bundle_folder(output_folder, customer, qr.label)
        except Exception as e:
            logger.warning("QBR: could not create customer bundle subfolder under Output: %s", e)
    target_folder_id = bundle_folder_id or output_folder

    out_title = f"{customer} — QBR ({qr.label})"
    copy_body: dict[str, Any] = {"name": out_title}
    if target_folder_id:
        copy_body["parents"] = [target_folder_id]

    try:
        copied = drive_svc.files().copy(
            fileId=template_id, body=copy_body, fields="id",
        ).execute()
        pres_id = copied["id"]
    except HttpError as e:
        logger.exception("QBR: copy template failed")
        return {"error": f"Drive copy failed: {e}", "customer": customer}

    url = f"https://docs.google.com/presentation/d/{pres_id}/edit"
    logger.info("QBR: copied template → %s", url)

    try:
        pres0 = slides_svc.presentations().get(presentationId=pres_id).execute()
    except HttpError as e:
        return {"error": f"Failed to read presentation: {e}", "presentation_id": pres_id, "customer": customer}

    slides0 = pres0.get("slides", [])
    if not slides0:
        return {"error": "Template copy has no slides", "presentation_id": pres_id, "customer": customer}

    inventory = build_slide_inventory(slides0)
    template_oids = frozenset(item["objectId"] for item in inventory)
    title_oid = slides0[0]["objectId"]

    oai = llm_client()
    plan = call_manifest_planner(oai, manifest_text, customer, inventory)
    logger.info(
        "QBR: manifest plan insert_executive_summary=%s notes=%s hide_indices=%s hide_titles=%s move_end=%s",
        plan.get("insert_executive_summary"),
        plan.get("notes", "")[:200],
        plan["hide"]["indices"],
        plan["hide"]["title_contains"],
        plan["move_to_end_title_contains"],
    )

    hide_oids = resolve_hide_object_ids(plan, inventory)

    from .qa import qa

    qa.begin(customer)
    report["_slides_svc"] = slides_svc
    report["_drive_svc"] = drive_svc

    exec_ids: list[str] = []
    exec_manifest_slides = 0
    exec_signals_page_ids: list[str] = []
    if plan.get("insert_executive_summary"):
        from .charts import DeckCharts

        report["_charts"] = DeckCharts(f"{customer} — QBR executive summary")
        try:
            exec_ids, exec_manifest_slides, exec_signals_page_ids = _insert_executive_summary_slides(
                slides_svc, pres_id, report, customer
            )
        except Exception as e:
            logger.exception("QBR: executive summary insert failed")
            return {
                "error": f"Executive summary insert failed: {e}",
                "presentation_id": pres_id,
                "url": url,
                "customer": customer,
            }
        logger.info(
            "QBR: inserted %d executive summary page(s) from %d manifest slide(s)",
            len(exec_ids),
            exec_manifest_slides,
        )
    else:
        logger.info("QBR: skipping executive summary insert (manifest plan insert_executive_summary=false)")

    exec_set = frozenset(exec_ids)

    try:
        _apply_slide_skipped(slides_svc, pres_id, hide_oids)
    except HttpError as e:
        logger.warning("QBR: apply isSkipped failed (continuing): %s", e)

    try:
        _apply_move_template_slides_to_end(
            slides_svc, pres_id, plan["move_to_end_title_contains"], template_oids
        )
    except HttpError as e:
        logger.warning("QBR: move_to_end failed (continuing): %s", e)

    try:
        pres_final = slides_svc.presentations().get(presentationId=pres_id).execute()
        final_slides = pres_final.get("slides", [])
    except HttpError as e:
        return {"error": f"Failed to re-read presentation: {e}", "url": url, "customer": customer}

    adapt_ids = compute_adapt_page_ids(final_slides, title_oid, exec_set)
    logger.info("QBR: adapting %d template slides (excludes title and any inserted exec block; hidden included)", len(adapt_ids))

    qbr_agenda_visual: dict[str, Any] = {"enabled": False, "skipped": True}
    if adapt_ids:
        run_qbr_adapt_hints_phase(
            oai,
            slides_svc,
            pres_id,
            final_slides,
            adapt_ids,
            customer,
            manifest_sha16=mf_hash,
        )
        adapt_custom_slides(
            slides_svc,
            pres_id,
            adapt_ids,
            report,
            oai,
            source_presentation_name=out_title,
            title_slide_object_id=title_oid,
            google_creds=_google_creds,
        )
        agenda_page_id = find_qbr_agenda_page_id(slides_svc, pres_id, adapt_ids, report)
        if agenda_page_id:
            qbr_agenda_visual = run_qbr_agenda_visual_refinement_loop(
                slides_svc,
                pres_id,
                agenda_page_id,
                report,
                oai,
                title_slide_object_id=title_oid,
            )
            logger.info(
                "QBR agenda visual refinement: %s",
                qbr_agenda_visual,
            )
        else:
            logger.warning(
                "QBR: no slide matched qbr_agenda hydrate (find_qbr_agenda_page_id returned None); "
                "QBR agenda visual refinement was not run."
            )
            qbr_agenda_visual = {
                "enabled": False,
                "skipped": True,
                "reason": "agenda_slide_not_found",
            }
        try:
            apply_qbr_template_style_strip_after_adapt(slides_svc, pres_id, adapt_ids)
        except Exception as e:
            logger.warning("QBR: post-adapt template style strip failed (slide may still show yellow/orange): %s", e)

    result: dict[str, Any] = {
        "ok": True,
        "customer": customer,
        "customer_query": customer_query,
        "presentation_id": pres_id,
        "url": url,
        "manifest_sha16": mf_hash,
        "insert_executive_summary": bool(plan.get("insert_executive_summary")),
        "slides_hidden": len(hide_oids),
        "exec_slides_inserted": len(exec_ids),
        "exec_manifest_slides": exec_manifest_slides,
        "adapt_slides": len(adapt_ids),
        "qbr_agenda_visual_refinement": qbr_agenda_visual,
        "plan_notes": plan.get("notes", ""),
        "qbr_output_folder_id": output_folder,
        "bundle_folder_id": bundle_folder_id,
    }

    if target_folder_id:
        result["companion_decks"] = _build_companion_decks_for_qbr_bundle(report, target_folder_id)
    else:
        result["companion_decks"] = []
        logger.info(
            "QBR: no Drive Output folder — skipping companion decks "
            "(set GOOGLE_QBR_GENERATOR_FOLDER_ID; optional GOOGLE_QBR_OUTPUT_PARENT_ID overrides Output parent)"
        )

    cohort_url = next(
        (
            c.get("url")
            for c in result.get("companion_decks") or []
            if c.get("deck_id") == "cohort_review" and c.get("url") and not c.get("error")
        ),
        None,
    )
    if cohort_url:
        if exec_signals_page_ids:
            n = apply_cohort_bundle_links_to_notable_signals(
                slides_svc, pres_id, cohort_url, page_object_ids=exec_signals_page_ids
            )
            if n:
                result["cohort_links_on_qbr_signals"] = n
        exec_companion_pid = next(
            (
                c.get("presentation_id")
                for c in result.get("companion_decks") or []
                if c.get("deck_id") == "executive_summary" and c.get("presentation_id") and not c.get("error")
            ),
            None,
        )
        if exec_companion_pid:
            n2 = apply_cohort_bundle_links_to_notable_signals(
                slides_svc, str(exec_companion_pid), cohort_url, page_object_ids=None
            )
            if n2:
                result["cohort_links_on_exec_summary_deck"] = n2

    return result
