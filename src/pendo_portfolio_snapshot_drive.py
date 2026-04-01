"""Google Drive cache for the Pendo portfolio crawl (cohort / portfolio decks).

**Wall clock:** Skipping ``get_portfolio_report`` saves Pendo/API time, but QBR bundle runs
four companion Slides decks *before* cohort. Those decks usually take longer than the
portfolio crawl, and the crawl previously ran *in parallel* with them—so end-to-end
bundle time may barely change until Slides work shrinks (or you tune
``BPO_SLIDES_WRITE_INTERVAL_SEC`` / chunk size). The filename **must** match the QBR
``days`` value (quarter length), e.g. ``decks --upload-portfolio-snapshot`` with no
``--days`` uses ``resolve_quarter().days``.

Snapshot folder resolution:
  1. ``BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID`` if set — explicit Drive folder id.
  2. Else ``Portfolio cache`` under ``GOOGLE_QBR_GENERATOR_FOLDER_ID`` (created if missing).

Other env (see ``config``):
  BPO_PORTFOLIO_SNAPSHOT_MAX_AGE_HOURS — max age of snapshot to accept (default 36)
  BPO_PORTFOLIO_SNAPSHOT_DISABLED=1  — never read snapshot; always compute from Pendo
  BPO_PORTFOLIO_SNAPSHOT_FORCE_REFRESH=1 — ignore snapshot for this process (compute + still can upload)
  BPO_PORTFOLIO_SNAPSHOT_AUTO_DAILY — when true (default), each QBR run ensures Drive has a snapshot
    for the current calendar day in BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ (default UTC); set to 0/false/off to disable.
  BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ — IANA zone for “today” (e.g. America/New_York).
"""

from __future__ import annotations

import json
import io
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from .config import (
    BPO_PORTFOLIO_SNAPSHOT_AUTO_DAILY,
    BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ,
    BPO_PORTFOLIO_SNAPSHOT_DISABLED,
    BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID,
    BPO_PORTFOLIO_SNAPSHOT_FORCE_REFRESH,
    BPO_PORTFOLIO_SNAPSHOT_MAX_AGE_HOURS,
    GOOGLE_QBR_GENERATOR_FOLDER_ID,
    logger,
)
from .drive_config import _find_or_create_folder, _get_drive, find_file_in_folder

PORTFOLIO_SNAPSHOT_SCHEMA_VERSION = 1
_SNAPSHOT_PREFIX = f"portfolio_snapshot_v{PORTFOLIO_SNAPSHOT_SCHEMA_VERSION}"
# Subfolder under GOOGLE_QBR_GENERATOR_FOLDER_ID when BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID is unset.
PORTFOLIO_SNAPSHOT_CACHE_FOLDER_NAME = "Portfolio cache"

_UNRESOLVED = object()
_resolved_generator_cache_folder_id: object | str | None = _UNRESOLVED


def resolve_portfolio_snapshot_folder_id() -> str | None:
    """Return Drive folder id for JSON snapshots, or None if not configured."""
    explicit = (BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID or "").strip()
    if explicit:
        return explicit
    global _resolved_generator_cache_folder_id
    if _resolved_generator_cache_folder_id is not _UNRESOLVED:
        out = _resolved_generator_cache_folder_id
        return out if isinstance(out, str) else None
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        _resolved_generator_cache_folder_id = None
        return None
    try:
        _resolved_generator_cache_folder_id = _find_or_create_folder(
            PORTFOLIO_SNAPSHOT_CACHE_FOLDER_NAME,
            GOOGLE_QBR_GENERATOR_FOLDER_ID,
        )
        logger.debug(
            "Portfolio snapshot: using folder %r under QBR generator (%s)",
            PORTFOLIO_SNAPSHOT_CACHE_FOLDER_NAME,
            _resolved_generator_cache_folder_id,
        )
    except Exception as e:
        logger.warning("Portfolio snapshot: could not open cache folder under QBR generator: %s", e)
        _resolved_generator_cache_folder_id = None
    out = _resolved_generator_cache_folder_id
    return out if isinstance(out, str) else None


def portfolio_snapshot_filename(days: int, max_customers: int | None) -> str:
    """Stable Drive object name for a (days, max_customers) portfolio snapshot."""
    cap = "all" if max_customers is None else f"max{int(max_customers)}"
    return f"{_SNAPSHOT_PREFIX}_days{int(days)}_{cap}.json"


def _build_envelope(
    report: dict[str, Any],
    days: int,
    max_customers: int | None,
) -> dict[str, Any]:
    return {
        "schema_version": PORTFOLIO_SNAPSHOT_SCHEMA_VERSION,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "days": int(days),
        "max_customers": max_customers,
        "report": report,
    }


def parse_portfolio_snapshot_envelope(
    raw: Any,
    *,
    expect_days: int,
    expect_max_customers: int | None,
) -> dict[str, Any] | None:
    """Validate envelope and return inner portfolio report, or None."""
    if not isinstance(raw, dict):
        return None
    if raw.get("schema_version") != PORTFOLIO_SNAPSHOT_SCHEMA_VERSION:
        return None
    if raw.get("days") != int(expect_days):
        return None
    mc = raw.get("max_customers")
    if mc is not None and not isinstance(mc, int):
        return None
    if expect_max_customers != mc:
        return None
    report = raw.get("report")
    if not isinstance(report, dict) or report.get("type") != "portfolio":
        return None
    if int(report.get("days") or 0) != int(expect_days):
        return None
    return report


def _snapshot_age_hours(saved_at: str | None, modified_time_rfc3339: str | None) -> float | None:
    """Return age in hours from saved_at ISO string or Drive modifiedTime."""
    if saved_at:
        try:
            s = saved_at.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
        except (TypeError, ValueError):
            pass
    if modified_time_rfc3339:
        try:
            s = modified_time_rfc3339.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
        except (TypeError, ValueError):
            pass
    return None


def _read_drive_file_text(file_id: str) -> str:
    drive = _get_drive()
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8")


def try_load_portfolio_snapshot_for_request(
    days: int,
    max_customers: int | None,
    *,
    max_age_hours: float | None = None,
) -> dict[str, Any] | None:
    """Load a fresh portfolio snapshot from Drive if configured and valid; else None."""
    if BPO_PORTFOLIO_SNAPSHOT_DISABLED:
        return None
    if BPO_PORTFOLIO_SNAPSHOT_FORCE_REFRESH:
        return None
    folder_id = resolve_portfolio_snapshot_folder_id()
    if not folder_id:
        logger.debug(
            "Portfolio snapshot: no folder (set GOOGLE_QBR_GENERATOR_FOLDER_ID or BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID)"
        )
        return None

    age_limit = BPO_PORTFOLIO_SNAPSHOT_MAX_AGE_HOURS if max_age_hours is None else max_age_hours
    name = portfolio_snapshot_filename(days, max_customers)

    try:
        file_id = find_file_in_folder(name, folder_id, mime_type=None)
        if not file_id:
            logger.info(
                "Portfolio snapshot: no file %r in Drive folder — upload a matching crawl, e.g. "
                "decks --upload-portfolio-snapshot --days %d (filename must match this quarter window)",
                name,
                days,
            )
            return None

        drive = _get_drive()
        meta = drive.files().get(fileId=file_id, fields="modifiedTime").execute()
        modified_time = meta.get("modifiedTime")

        text = _read_drive_file_text(file_id)
        data = json.loads(text)
        report = parse_portfolio_snapshot_envelope(
            data, expect_days=days, expect_max_customers=max_customers
        )
        if report is None:
            logger.warning("Portfolio snapshot: envelope invalid for %r", name)
            return None

        saved_at = data.get("saved_at") if isinstance(data, dict) else None
        age_h = _snapshot_age_hours(saved_at if isinstance(saved_at, str) else None, modified_time)
        if age_h is None:
            logger.warning("Portfolio snapshot: could not determine age for %r — rejecting", name)
            return None
        if age_h > age_limit:
            logger.info(
                "Portfolio snapshot: %r is stale (%.1fh > %.1fh) — recomputing from Pendo",
                name,
                age_h,
                age_limit,
            )
            return None

        logger.info(
            "Portfolio snapshot: using Drive file %r (age %.1fh, %d customers)",
            name,
            age_h,
            report.get("customer_count", 0),
        )
        return report
    except Exception as e:
        logger.warning("Portfolio snapshot: Drive read failed (%s) — falling back to Pendo", e)
        return None


def _snapshot_calendar_zone() -> ZoneInfo:
    try:
        return ZoneInfo(BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ)
    except Exception:
        logger.warning(
            "Portfolio snapshot: invalid BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ %r — using UTC",
            BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ,
        )
        return ZoneInfo("UTC")


def _calendar_today_for_snapshot() -> date:
    return datetime.now(_snapshot_calendar_zone()).date()


def saved_at_to_calendar_date(saved_at: str) -> date | None:
    """Parse envelope ``saved_at`` ISO string to a calendar date in the configured TZ."""
    try:
        s = saved_at.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_snapshot_calendar_zone()).date()
    except (TypeError, ValueError):
        return None


def ensure_daily_portfolio_snapshot_for_qbr(days: int, max_customers: int | None = None) -> None:
    """If enabled and a snapshot folder exists, ensure today's JSON exists (else compute + upload).

    Called after ``PendoClient.preload`` on QBR so portfolio crawl reuses warm caches.
    Failures are logged; QBR continues without snapshot for this run.
    """
    if BPO_PORTFOLIO_SNAPSHOT_DISABLED:
        return
    if not BPO_PORTFOLIO_SNAPSHOT_AUTO_DAILY:
        return
    folder_id = resolve_portfolio_snapshot_folder_id()
    if not folder_id:
        return

    import time

    name = portfolio_snapshot_filename(days, max_customers)
    today = _calendar_today_for_snapshot()

    try:
        file_id = find_file_in_folder(name, folder_id, mime_type=None)
        if file_id:
            text = _read_drive_file_text(file_id)
            data = json.loads(text)
            if isinstance(data, dict) and data.get("schema_version") == PORTFOLIO_SNAPSHOT_SCHEMA_VERSION:
                sas = data.get("saved_at")
                if isinstance(sas, str):
                    sd = saved_at_to_calendar_date(sas)
                    if sd is not None and sd >= today:
                        logger.info(
                            "Portfolio snapshot: %r already saved for calendar day %s (%s) — skip auto-upload",
                            name,
                            sd.isoformat(),
                            BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ,
                        )
                        return
    except Exception as e:
        logger.info("Portfolio snapshot: auto-upload will run (existing file unreadable or not for today): %s", e)

    t0 = time.perf_counter()
    logger.info(
        "QBR: auto-uploading portfolio snapshot %r (new calendar day or missing in %s)...",
        name,
        BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ,
    )
    try:
        from .pendo_client import PendoClient

        client = PendoClient()
        report = client.get_portfolio_report(days=days, max_customers=max_customers)
        upload_portfolio_snapshot_to_drive(report, folder_id, days, max_customers)
        logger.info(
            "QBR: portfolio snapshot %r uploaded in %.1fs (%d customers)",
            name,
            time.perf_counter() - t0,
            report.get("customer_count", 0),
        )
    except Exception as e:
        logger.warning("QBR: portfolio snapshot auto-upload failed (continuing QBR): %s", e)


def upload_portfolio_snapshot_to_drive(
    report: dict[str, Any],
    folder_id: str,
    days: int,
    max_customers: int | None,
) -> str:
    """Serialize *report* (from ``get_portfolio_report``) and create or replace the snapshot file."""
    if report.get("type") != "portfolio":
        raise ValueError("report must be a portfolio dict from get_portfolio_report")
    envelope = _build_envelope(report, days, max_customers)
    payload = json.dumps(envelope, ensure_ascii=False, indent=2, default=str)
    name = portfolio_snapshot_filename(days, max_customers)

    drive = _get_drive()
    media = MediaIoBaseUpload(
        io.BytesIO(payload.encode("utf-8")),
        mimetype="application/json",
    )
    existing_id = find_file_in_folder(name, folder_id, mime_type=None)
    if existing_id:
        f = drive.files().update(fileId=existing_id, media_body=media, fields="id").execute()
        logger.info("Portfolio snapshot: updated Drive file %r (%s)", name, f["id"])
        return f["id"]
    meta: dict[str, Any] = {"name": name, "parents": [folder_id]}
    f = drive.files().create(body=meta, media_body=media, fields="id").execute()
    logger.info("Portfolio snapshot: created Drive file %r (%s)", name, f["id"])
    return f["id"]


def run_upload_portfolio_snapshot_cli(days: int, max_customers: int | None) -> dict[str, Any]:
    """Compute portfolio from Pendo and upload to the resolved snapshot folder (see module docstring)."""
    folder_id = resolve_portfolio_snapshot_folder_id()
    if not folder_id:
        return {
            "error": (
                "No snapshot folder: set BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID or "
                "GOOGLE_QBR_GENERATOR_FOLDER_ID (cache uses subfolder "
                f"{PORTFOLIO_SNAPSHOT_CACHE_FOLDER_NAME!r})"
            ),
        }

    from .pendo_client import PendoClient

    client = PendoClient()
    report = client.get_portfolio_report(days=days, max_customers=max_customers)
    fid = upload_portfolio_snapshot_to_drive(report, folder_id, days, max_customers)
    return {
        "file_id": fid,
        "filename": portfolio_snapshot_filename(days, max_customers),
        "customer_count": report.get("customer_count", 0),
    }
