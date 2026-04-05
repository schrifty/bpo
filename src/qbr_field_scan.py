"""Fast inventory of data fields referenced across QBR Google Slides (scan-only).

Reads slide text (+ optional thumbnails), runs the same broad slide analysis as hydrate
(full LLM output including ``charts`` / interpretation), upserts unique ``data_ask``
field names into SQLite, and **writes the full analysis** to the same on-disk cache
(``.slide_cache/analysis/``) that hydrate uses — so a prior ``--scan-fields`` run warms
cache for speaker notes without re-calling the LLM on hydrate. Does not copy decks or
apply slide replacements.

Usage:
    decks --scan-fields [--db PATH] [--no-thumbnail] [--workers N] [--no-progress]
                          [-- <presentation-id-or-url> ...]
    decks --list-fields [--db PATH]
    decks --clear-fields [--db PATH]
"""

from __future__ import annotations

import os
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .config import HYDRATE_MAX_SLIDES, logger
from .evaluate import (
    _analyze_slide_broad,
    _collect_hydrate_intake_presentations,
    _describe_elements,
    _extract_text,
    _get_slide_thumbnail_b64,
    _set_cached_slide_analysis,
    _slide_content_hash,
)
from .slides_client import _get_service


def _scan_max_slides_per_deck() -> int | None:
    """Max slides analyzed per presentation.

    Uses ``HYDRATE_MAX_SLIDES`` (same cap as hydrate) unless ``SCAN_MAX_SLIDES`` is set
    in the environment, in which case that value wins (``0`` = no limit for scan).
    """
    raw = os.environ.get("SCAN_MAX_SLIDES", "").strip()
    if raw:
        try:
            n = int(raw)
            return n if n > 0 else None
        except ValueError:
            pass
    if HYDRATE_MAX_SLIDES > 0:
        return HYDRATE_MAX_SLIDES
    return None


# Coarse types aligned with canonical `data_ask` keys in evaluate.
_CANONICAL_TYPES: dict[str, str] = {
    "customer_name": "text",
    "report_date": "date",
    "quarter": "text",
    "quarter_start": "date",
    "quarter_end": "date",
    "total_users": "integer",
    "active_users": "integer",
    "total_sites": "integer",
    "active_sites": "integer",
    "account_total_minutes": "integer",
    "account_avg_weekly_hours": "integer",
    "total_shortages": "integer",
    "total_critical_shortages": "integer",
    "weekly_active_buyers_pct_avg": "percent",
    "health_score": "text",
    "site_details": "json_array",
    "cs_health_sites": "json_array",
    "support": "json_object",
    "salesforce": "json_object",
    "platform_value": "json_object",
    "supply_chain": "json_object",
}


def normalize_field_name(key: str) -> str:
    return (key or "").strip().replace(" ", "_").replace("-", "_").lower()


def infer_field_type(key: str) -> str:
    """Best-effort semantic type for a `data_ask` key (slug or canonical)."""
    k = normalize_field_name(key)
    if k.startswith("_embedded"):
        return "embed_chart" if "chart" in k else "embed_image"
    if k in _CANONICAL_TYPES:
        return _CANONICAL_TYPES[k]
    low = k.lower()
    if any(x in low for x in ("percent", "_pct")) or low.endswith("pct"):
        return "percent"
    if "date" in low or "quarter" in low or low.endswith("_at"):
        return "date"
    if any(x in low for x in ("revenue", "arr", "cost", "price", "spend", "savings", "value_usd")):
        return "currency"
    if any(
        x in low
        for x in (
            "count",
            "total_tickets",
            "open",
            "resolved",
            "sites",
            "users",
            "number",
            "nps",
        )
    ):
        return "integer"
    return "unknown"


_PRES_ID_RE = re.compile(
    r"(?:presentation/d/|/d/)([a-zA-Z0-9_-]+)",
    re.I,
)


def parse_presentation_id(token: str) -> str | None:
    """Accept raw ID or a full Google Slides URL."""
    t = (token or "").strip()
    if not t:
        return None
    if re.fullmatch(r"[a-zA-Z0-9_-]+", t):
        return t
    m = _PRES_ID_RE.search(t)
    return m.group(1) if m else None


DEFAULT_SCAN_DB = "qbr_fields.db"


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS qbr_data_fields (
            field_name TEXT PRIMARY KEY NOT NULL,
            field_type TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _upsert_field(conn: sqlite3.Connection, field_name: str, field_type: str) -> None:
    conn.execute(
        """
        INSERT INTO qbr_data_fields (field_name, field_type) VALUES (?, ?)
        ON CONFLICT(field_name) DO UPDATE SET field_type = CASE
            WHEN excluded.field_type = qbr_data_fields.field_type
                THEN qbr_data_fields.field_type
            ELSE 'mixed'
        END
        """,
        (field_name, field_type),
    )


def _collect_jobs_for_presentation(
    slides_svc: Any,
    pres_id: str,
    pres_name: str,
    *,
    use_thumbnails: bool,
    max_slides: int | None,
) -> list[tuple[str, str, int, int, str, dict[str, Any], str | None, str]]:
    """Build (name, pres_id, slide_num, total, text, elements, thumb_b64, page_id) jobs."""
    full = slides_svc.presentations().get(presentationId=pres_id).execute()
    slides = full.get("slides") or []
    if max_slides is not None:
        slides = slides[:max_slides]
    total = len(slides)
    jobs: list[tuple[str, str, int, int, str, dict[str, Any], str | None, str]] = []
    for si, slide in enumerate(slides, 1):
        texts: list[str] = []
        for el in slide.get("pageElements", []):
            texts.extend(_extract_text(el))
        slide_text = "\n".join(texts)
        elements = _describe_elements(slide)
        page_id = slide.get("objectId", "")
        thumb_b64: str | None = None
        if use_thumbnails and page_id:
            try:
                thumb_b64 = _get_slide_thumbnail_b64(slides_svc, pres_id, page_id)
            except Exception as e:
                logger.warning("scan: thumbnail failed slide %s: %s", si, e)
        jobs.append((pres_name, pres_id, si, total, slide_text, elements, thumb_b64, page_id))
    return jobs


def _run_analysis_job(
    oai: Any,
    job: tuple[str, str, int, int, str, dict[str, Any], str | None, str],
) -> list[dict[str, Any]]:
    pres_name, _pres_id, si, total, slide_text, elements, thumb_b64, page_id = job
    analysis = _analyze_slide_broad(
        oai, slide_text, elements, thumb_b64, si, total, pres_name
    )
    # Same cache key + path as hydrate / evaluate — full analysis (charts, interpretation)
    cache_key = _slide_content_hash(
        thumb_b64, slide_text[:2000] if slide_text else "", page_id=page_id
    )
    if cache_key:
        _set_cached_slide_analysis(cache_key, analysis)
    return list(analysis.get("data_ask") or [])


def scan_presentations_to_sqlite(
    db_path: str | Path,
    presentations: list[dict[str, Any]],
    *,
    use_thumbnails: bool = True,
    workers: int = 4,
    max_slides_per_deck: int | None = None,
    show_progress: bool = True,
) -> dict[str, Any]:
    """Scan one or more presentations and merge `data_ask` field keys into SQLite.

    `presentations` items must include ``id`` and ``name`` (for logging / analysis context).
    """
    db_path = Path(db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    try:
        _init_db(conn)
        slides_svc, _drive, _ = _get_service()
        from .config import llm_client

        oai = llm_client()

        all_jobs: list[tuple[str, str, int, int, str, dict[str, Any], str | None, str]] = []
        for p in presentations:
            pid = p.get("id") or p.get("presentation_id")
            pname = p.get("name") or pid or "presentation"
            if not pid:
                continue
            jobs = _collect_jobs_for_presentation(
                slides_svc,
                str(pid),
                str(pname),
                use_thumbnails=use_thumbnails,
                max_slides=max_slides_per_deck,
            )
            all_jobs.extend(jobs)

        n_slides = len(all_jobs)
        n_fields_seen = 0
        workers = max(1, min(workers, 16))

        if not all_jobs:
            conn.commit()
            return {"db": str(db_path), "slides": 0, "fields_unique": 0, "fields_raw": 0}

        from tqdm import tqdm

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run_analysis_job, oai, j): j for j in all_jobs}
            n_fut = len(futures)
            pbar = tqdm(
                total=n_fut,
                unit="slide",
                desc="Scanning slides",
                file=sys.stderr,
                disable=not show_progress,
            )
            try:
                for fut in as_completed(futures):
                    try:
                        data_ask = fut.result()
                    except Exception as e:
                        job = futures[fut]
                        logger.warning("scan: analysis failed for %s slide %s: %s", job[0], job[2], e)
                        pbar.update(1)
                        continue
                    for item in data_ask:
                        key = item.get("key") if isinstance(item, dict) else None
                        if not key or not str(key).strip():
                            continue
                        fname = normalize_field_name(str(key))
                        if not fname:
                            continue
                        ftyp = infer_field_type(fname)
                        _upsert_field(conn, fname, ftyp)
                        n_fields_seen += 1
                    pbar.update(1)
            finally:
                pbar.close()

        conn.commit()
        row = conn.execute("SELECT COUNT(*) FROM qbr_data_fields").fetchone()
        n_unique = int(row[0]) if row else 0
        return {
            "db": str(db_path),
            "slides": n_slides,
            "fields_raw": n_fields_seen,
            "fields_unique": n_unique,
        }
    finally:
        conn.close()


def list_scan_db(db_path: str | Path) -> None:
    """Print all rows from the scan DB (field_name, field_type), sorted by name."""
    db_path = Path(db_path).resolve()
    if not db_path.exists():
        print(f"0 fields  {db_path}  (not created yet — run `decks --scan-fields` to populate)")
        return
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            "SELECT field_name, field_type FROM qbr_data_fields ORDER BY field_name"
        )
        rows = cur.fetchall()
        if not rows:
            print(f"(empty) {db_path}")
            return
        w = max(len(r[0]) for r in rows)
        for name, typ in rows:
            print(f"{name:{w}s}  {typ}")
        print(f"{len(rows)} row(s)  {db_path}")
    finally:
        conn.close()


def clear_scan_db(db_path: str | Path) -> int:
    """Remove all rows from `qbr_data_fields`. Returns deleted row count."""
    db_path = Path(db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    try:
        _init_db(conn)
        cur = conn.execute("SELECT COUNT(*) FROM qbr_data_fields")
        n_before = int(cur.fetchone()[0])
        conn.execute("DELETE FROM qbr_data_fields")
        conn.commit()
        return n_before
    finally:
        conn.close()


def db_path_from_argv(argv: list[str], default: str = DEFAULT_SCAN_DB) -> str:
    """Return the value after ``--db``, or ``default``."""
    args = argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--db" and i + 1 < len(args):
            return args[i + 1]
        i += 1
    return default


def run_list_fields_cli(argv: list[str]) -> None:
    """CLI entry: ``decks --list-fields [--db PATH]``."""
    list_scan_db(db_path_from_argv(argv))


def run_clear_fields_cli(argv: list[str]) -> None:
    """CLI entry: ``decks --clear-fields [--db PATH]``."""
    db = db_path_from_argv(argv)
    n = clear_scan_db(db)
    print(f"Cleared {n} row(s) from {Path(db).resolve()}")


def run_scan_cli(argv: list[str]) -> None:
    """CLI entry: ``decks --scan-fields ...``."""
    args = [a for a in argv[1:] if a != "--scan-fields"]
    db_path = DEFAULT_SCAN_DB
    use_thumbnails = True
    workers = 4
    show_progress = True
    pres_tokens: list[str] = []

    i = 0
    while i < len(args):
        a = args[i]
        if a == "--db" and i + 1 < len(args):
            db_path = args[i + 1]
            i += 2
            continue
        if a == "--workers" and i + 1 < len(args):
            try:
                workers = max(1, int(args[i + 1]))
            except ValueError:
                workers = 4
            i += 2
            continue
        if a == "--no-progress":
            show_progress = False
            i += 1
            continue
        if a == "--no-thumbnail":
            use_thumbnails = False
            i += 1
            continue
        if a == "--":
            pres_tokens.extend(args[i + 1 :])
            break
        if not a.startswith("-"):
            pres_tokens.append(a)
        i += 1

    max_slides = _scan_max_slides_per_deck()
    env_scan = os.environ.get("SCAN_MAX_SLIDES", "").strip()
    if env_scan:
        cap_src = "SCAN_MAX_SLIDES"
    elif HYDRATE_MAX_SLIDES > 0:
        cap_src = "HYDRATE_MAX_SLIDES"
    else:
        cap_src = "no cap (HYDRATE_MAX_SLIDES=0)"

    presentations: list[dict[str, Any]] = []
    if pres_tokens:
        for tok in pres_tokens:
            pid = parse_presentation_id(tok)
            if pid:
                presentations.append({"id": pid, "name": tok[:80]})
            else:
                print(f"Could not parse presentation id from: {tok!r}", file=sys.stderr)
    else:
        batch, msg = _collect_hydrate_intake_presentations(log_prefix="scan")
        if msg:
            print(msg, file=sys.stderr)
            sys.exit(1)
        if not batch:
            print("No presentations to scan.", file=sys.stderr)
            sys.exit(1)
        presentations = batch

    print(f"Scanning {len(presentations)} presentation(s) → {db_path}")
    print(
        f"  thumbnails: {use_thumbnails}  workers: {workers}  "
        f"max_slides/deck: {max_slides or 'all'} ({cap_src})"
    )
    stats = scan_presentations_to_sqlite(
        db_path,
        presentations,
        use_thumbnails=use_thumbnails,
        workers=workers,
        max_slides_per_deck=max_slides,
        show_progress=show_progress,
    )
    print(f"  slides analyzed: {stats['slides']}")
    print(f"  data_ask entries: {stats['fields_raw']}")
    print(f"  unique fields in DB: {stats['fields_unique']}")
    print(f"  sqlite: {stats['db']}")
