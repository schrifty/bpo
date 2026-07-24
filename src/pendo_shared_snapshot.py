"""Shared multi-window Pendo ingest snapshot (pull once, transform many).

Nightly ``pendo-snapshot-refresh`` warms disk preload slices for the windows used by
export transforms (7/14/30/60/90), writes a manifest under ``CORTEX_CACHE_DIR/pendo/``,
and uploads the 90d Drive portfolio rollup for deck reuse.

Transforms that set ``CORTEX_PENDO_SNAPSHOT_REQUIRE`` fail loud when the manifest is
missing, too old, or does not cover the required windows — they do not silently
re-crawl from a cold cache as if ingest succeeded.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import config as _config
from .config import logger
from .pendo_cache import (
    PRELOAD_KIND_FEATURE_CATALOG,
    PRELOAD_KIND_FEATURE_EVENTS,
    PRELOAD_KIND_GUIDE_CATALOG,
    PRELOAD_KIND_GUIDE_EVENTS,
    PRELOAD_KIND_PAGE_CATALOG,
    PRELOAD_KIND_PAGE_EVENTS,
    PRELOAD_KIND_TRACK_EVENTS,
    PRELOAD_KIND_USAGE_BY_SITE,
    PRELOAD_KIND_USAGE_BY_SITE_ENTITY,
    PRELOAD_KIND_VISITORS,
    disk_cache_enabled,
    preload_cache_key,
    try_load_preload_payload,
)

SCHEMA_VERSION = 1
MANIFEST_FILENAME = f"shared_snapshot_manifest_v{SCHEMA_VERSION}.json"

# Windows used by scheduled transforms:
# - export-nightly: 90
# - ford-pendo-7d (7+7 compare): 14
# - ford-pendo-30d / top-arr (30+30): 60
# Plus exact 7/30 for any single-window callers.
DEFAULT_REFRESH_WINDOWS: tuple[int, ...] = (7, 14, 30, 60, 90)

_DAY_KINDS = (
    PRELOAD_KIND_VISITORS,
    PRELOAD_KIND_FEATURE_EVENTS,
    PRELOAD_KIND_PAGE_EVENTS,
    PRELOAD_KIND_TRACK_EVENTS,
    PRELOAD_KIND_GUIDE_EVENTS,
    PRELOAD_KIND_USAGE_BY_SITE,
    PRELOAD_KIND_USAGE_BY_SITE_ENTITY,
)
_CATALOG_KINDS = (
    PRELOAD_KIND_PAGE_CATALOG,
    PRELOAD_KIND_FEATURE_CATALOG,
    PRELOAD_KIND_GUIDE_CATALOG,
)


class PendoSnapshotError(RuntimeError):
    """Shared Pendo snapshot missing, stale, incomplete, or refresh failed."""


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def snapshot_require_enabled() -> bool:
    return _truthy("CORTEX_PENDO_SNAPSHOT_REQUIRE")


def snapshot_max_age_hours() -> float:
    raw = (os.environ.get("CORTEX_PENDO_SNAPSHOT_MAX_AGE_HOURS") or "").strip()
    if not raw:
        return float(getattr(_config, "CORTEX_PENDO_SNAPSHOT_MAX_AGE_HOURS", 18.0))
    try:
        return max(0.0, float(raw))
    except ValueError:
        return float(getattr(_config, "CORTEX_PENDO_SNAPSHOT_MAX_AGE_HOURS", 18.0))


def parse_windows(raw: str | None) -> list[int]:
    if not (raw or "").strip():
        return []
    out: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        out.append(max(1, int(part)))
    return sorted(set(out))


def required_windows_from_env(*, fallback: Sequence[int] | None = None) -> list[int]:
    parsed = parse_windows(os.environ.get("CORTEX_PENDO_SNAPSHOT_REQUIRE_WINDOWS"))
    if parsed:
        return parsed
    if fallback:
        return sorted({max(1, int(d)) for d in fallback})
    return []


def manifest_path() -> Path:
    return Path(_config.CORTEX_CACHE_ROOT) / "pendo" / MANIFEST_FILENAME


def load_manifest() -> dict[str, Any] | None:
    path = manifest_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def write_manifest(payload: dict[str, Any]) -> Path:
    path = manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


def _manifest_age_hours(manifest: dict[str, Any], *, now: float | None = None) -> float | None:
    saved_at = manifest.get("saved_at")
    if not saved_at:
        ts = manifest.get("ts")
        if ts is None:
            return None
        try:
            return max(0.0, (now or time.time()) - float(ts)) / 3600.0
        except (TypeError, ValueError):
            return None
    try:
        dt = datetime.fromisoformat(str(saved_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, ((now or time.time()) - dt.timestamp()) / 3600.0)


def missing_preload_keys(windows: Iterable[int]) -> list[str]:
    """Return disk-cache keys that should exist after a successful refresh but do not."""
    missing: list[str] = []
    for kind in _CATALOG_KINDS:
        if try_load_preload_payload(kind, None) is None:
            missing.append(preload_cache_key(kind, None))
    for days in sorted({max(1, int(d)) for d in windows}):
        for kind in _DAY_KINDS:
            if try_load_preload_payload(kind, days) is None:
                missing.append(preload_cache_key(kind, days))
    return missing


def check_shared_pendo_snapshot(
    *,
    required_windows: Sequence[int],
    max_age_hours: float | None = None,
) -> dict[str, Any]:
    """Validate manifest coverage/age. Raises :class:`PendoSnapshotError` on failure."""
    windows = sorted({max(1, int(d)) for d in required_windows})
    if not windows:
        raise PendoSnapshotError("shared Pendo snapshot require: no windows specified")
    if not disk_cache_enabled():
        raise PendoSnapshotError(
            "shared Pendo snapshot require: disk cache is disabled "
            "(CORTEX_PENDO_DISK_CACHE_TTL_HOURS / CORTEX_PENDO_DISK_CACHE_DISABLED)"
        )

    manifest = load_manifest()
    if not manifest:
        raise PendoSnapshotError(
            f"shared Pendo snapshot require: missing manifest at {manifest_path()} "
            "(run pendo-snapshot-refresh / cortex --refresh-pendo-snapshot first)"
        )

    age_h = _manifest_age_hours(manifest)
    cap = snapshot_max_age_hours() if max_age_hours is None else max(0.0, float(max_age_hours))
    if age_h is None:
        raise PendoSnapshotError("shared Pendo snapshot require: manifest missing saved_at/ts")
    if age_h > cap:
        raise PendoSnapshotError(
            f"shared Pendo snapshot require: manifest age {age_h:.1f}h exceeds max {cap:.1f}h "
            f"(saved_at={manifest.get('saved_at')!r})"
        )

    covered = {int(d) for d in (manifest.get("windows") or []) if str(d).strip().isdigit() or isinstance(d, int)}
    missing_windows = [d for d in windows if d not in covered]
    if missing_windows:
        raise PendoSnapshotError(
            f"shared Pendo snapshot require: manifest missing window(s) {missing_windows} "
            f"(have {sorted(covered)})"
        )

    missing_keys = missing_preload_keys(windows)
    if missing_keys:
        preview = missing_keys[:12]
        more = f" (+{len(missing_keys) - 12} more)" if len(missing_keys) > 12 else ""
        raise PendoSnapshotError(
            f"shared Pendo snapshot require: disk cache missing keys {preview}{more} "
            "(manifest present but preload slices expired or incomplete)"
        )

    return {
        "ok": True,
        "age_h": round(age_h, 2),
        "max_age_h": cap,
        "windows": windows,
        "manifest_path": str(manifest_path()),
        "saved_at": manifest.get("saved_at"),
    }


def ensure_shared_pendo_snapshot(*, required_windows: Sequence[int]) -> dict[str, Any] | None:
    """If ``CORTEX_PENDO_SNAPSHOT_REQUIRE`` is set, validate snapshot; else no-op."""
    if not snapshot_require_enabled():
        return None
    return check_shared_pendo_snapshot(required_windows=required_windows)


def refresh_shared_pendo_snapshot(
    *,
    windows: Sequence[int] | None = None,
    upload_portfolio_days: int | None = 90,
    pc: Any | None = None,
) -> dict[str, Any]:
    """Warm disk preload for ``windows``, optionally upload Drive portfolio snapshot, write manifest.

    Fails loud on preload gaps or Drive upload errors (when upload is requested).
    """
    if not disk_cache_enabled():
        raise PendoSnapshotError(
            "Cannot refresh shared Pendo snapshot: disk cache disabled "
            "(set CORTEX_PENDO_DISK_CACHE_TTL_HOURS > 0)"
        )

    win = sorted({max(1, int(d)) for d in (windows if windows is not None else DEFAULT_REFRESH_WINDOWS)})
    if not win:
        raise PendoSnapshotError("refresh windows must be non-empty")

    from .pendo_client import PendoClient

    client = pc if pc is not None else PendoClient()
    t0 = time.time()
    window_timings: dict[str, float] = {}

    for days in win:
        logger.info("Pendo shared snapshot: warming window days=%d", days)
        w0 = time.time()
        client.preload(days)
        # preload() does not fan out entity-level usage; warm it explicitly.
        client._get_usage_by_site_entity_cached(days)
        window_timings[str(days)] = round(time.time() - w0, 1)

    missing = missing_preload_keys(win)
    if missing:
        preview = missing[:12]
        more = f" (+{len(missing) - 12} more)" if len(missing) > 12 else ""
        raise PendoSnapshotError(
            f"Pendo shared snapshot refresh incomplete: missing disk keys {preview}{more}"
        )

    portfolio: dict[str, Any] | None = None
    if upload_portfolio_days is not None and int(upload_portfolio_days) > 0:
        from .pendo_portfolio_snapshot_drive import run_upload_portfolio_snapshot_cli

        days_up = int(upload_portfolio_days)
        logger.info("Pendo shared snapshot: uploading Drive portfolio rollup days=%d", days_up)
        portfolio = run_upload_portfolio_snapshot_cli(days_up, None)
        if portfolio.get("error"):
            raise PendoSnapshotError(
                f"Pendo shared snapshot Drive portfolio upload failed: {portfolio['error']}"
            )

    now = time.time()
    saved_at = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "saved_at": saved_at,
        "ts": now,
        "windows": win,
        "upload_portfolio_days": int(upload_portfolio_days) if upload_portfolio_days else None,
        "portfolio": portfolio,
        "window_timings_s": window_timings,
        "duration_s": round(now - t0, 1),
        "disk_cache_ttl_h": round(_config.CORTEX_PENDO_DISK_CACHE_TTL_SECONDS / 3600.0, 2),
    }
    path = write_manifest(manifest)
    logger.info(
        "Pendo shared snapshot: wrote manifest %s windows=%s duration_s=%.1f",
        path,
        win,
        manifest["duration_s"],
    )
    return manifest


def refresh_pendo_snapshot_main(argv: list[str] | None = None, *, prog: str = "cortex --refresh-pendo-snapshot") -> int:
    """CLI entry for shared snapshot refresh."""
    import argparse

    ap = argparse.ArgumentParser(prog=prog, description="Warm shared multi-window Pendo disk snapshot for nightly transforms.")
    ap.add_argument(
        "--windows",
        default=",".join(str(d) for d in DEFAULT_REFRESH_WINDOWS),
        help=f"Comma-separated day windows to preload (default: {','.join(str(d) for d in DEFAULT_REFRESH_WINDOWS)})",
    )
    ap.add_argument(
        "--upload-portfolio-days",
        type=int,
        default=90,
        help="Also upload Drive portfolio_snapshot for this window (0 to skip). Default: 90",
    )
    args = ap.parse_args(argv)
    windows = parse_windows(args.windows)
    upload_days = None if int(args.upload_portfolio_days) <= 0 else int(args.upload_portfolio_days)
    try:
        manifest = refresh_shared_pendo_snapshot(windows=windows, upload_portfolio_days=upload_days)
    except PendoSnapshotError as exc:
        logger.error("%s", exc)
        print(f"error: {exc}", flush=True)
        return 1
    print(
        "CORTEX_PENDO_SNAPSHOT="
        + json.dumps(
            {
                "saved_at": manifest.get("saved_at"),
                "windows": manifest.get("windows"),
                "duration_s": manifest.get("duration_s"),
                "portfolio_file_id": (manifest.get("portfolio") or {}).get("file_id"),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    return 0
