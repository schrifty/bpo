"""Google Slides + Drive API: auth, batchUpdate throttling, chunked updates."""

import json
import os
import random
import threading
import time
from pathlib import Path
from typing import Any

import httplib2
from google.oauth2 import service_account
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

GOOGLE_API_TIMEOUT_S = 120

from .config import GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_DRIVE_OWNER_EMAIL, logger

SCOPES = [
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive",
]

def _get_service():
    """Build authenticated Slides + Drive API services."""
    creds = None
    creds_path = GOOGLE_APPLICATION_CREDENTIALS
    if creds_path:
        path = Path(creds_path)
        if path.exists():
            creds = service_account.Credentials.from_service_account_file(
                str(path), scopes=SCOPES
            )
            try:
                with open(path) as f:
                    proj_id = json.load(f).get("project_id")
                if proj_id:
                    creds = creds.with_quota_project(proj_id)
            except Exception:
                pass
            if GOOGLE_DRIVE_OWNER_EMAIL:
                owner = GOOGLE_DRIVE_OWNER_EMAIL.strip()
                if owner:
                    creds = creds.with_subject(owner)
                    logger.debug("Impersonating %s (domain-wide delegation)", owner)
            logger.debug("Using service account: %s", creds_path)
    if creds is None:
        try:
            import google.auth
            creds, _ = google.auth.default(scopes=SCOPES)
        except Exception as e:
            raise ValueError(
                "No valid credentials. Set GOOGLE_APPLICATION_CREDENTIALS or run: gcloud auth application-default login"
            ) from e
    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=GOOGLE_API_TIMEOUT_S))
    return (
        build("slides", "v1", http=http),
        build("drive", "v3", http=http),
        creds,
    )


def _build_slides_service_for_thread(creds) -> Any:
    """Create an independent Slides service with its own HTTP transport.

    Each instance has a private ``httplib2.Http`` so it's safe to use from
    any thread without locking.  Use this for parallel read-only operations
    like thumbnail URL fetching.
    """
    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=GOOGLE_API_TIMEOUT_S))
    return build("slides", "v1", http=http)


def _google_api_unreachable_hint(exc: BaseException) -> str | None:
    """If *exc* looks like DNS/network failure reaching Google, return guidance for operators."""
    err = str(exc).lower()
    markers = (
        "oauth2.googleapis.com",
        "unable to find the server",
        "failed to establish a new connection",
        "name or service not known",
        "nodename nor servname provided, or not known",
        "connection refused",
        "network is unreachable",
        "temporary failure in name resolution",
        "getaddrinfo failed",
    )
    if any(m in err for m in markers):
        return (
            "Cannot reach Google APIs (often oauth2.googleapis.com for token exchange). "
            "Check internet, VPN, DNS, and firewall. Service accounts still need outbound HTTPS "
            "to trade a JWT for access tokens before Slides/Drive calls succeed."
        )
    return None


# Google Slides presentations.batchUpdate hard limit (documented): 100_000 entries in `requests`.
_GOOGLE_SLIDES_MAX_SUBREQUESTS_PER_BATCH = 100_000
# Stay far below: large CS decks can queue 100k+ ops total across many small batches.
_SLIDES_BATCH_UPDATE_DEFAULT_CHUNK = 2_000
_SLIDES_BATCH_UPDATE_MAX_CHUNK = 5_000

# Quota: WriteRequestsPerMinutePerUser ≈ 60. Space batchUpdate calls to avoid 429 bursts.
_slides_write_lock = threading.Lock()
_slides_last_write_mono: float = 0.0


def _slides_write_interval_sec() -> float:
    raw = os.environ.get("BPO_SLIDES_WRITE_INTERVAL_SEC", "1.05").strip()
    try:
        v = float(raw)
    except ValueError:
        return 1.05
    return max(0.0, v)


def _slides_batch_update_max_retries() -> int:
    raw = os.environ.get("BPO_SLIDES_BATCH_UPDATE_RETRIES", "12").strip()
    try:
        n = int(raw)
    except ValueError:
        return 12
    return max(1, min(n, 30))


def _throttle_before_slides_write() -> None:
    """Ensure a minimum gap between successful Slides write calls (batchUpdate)."""
    interval = _slides_write_interval_sec()
    if interval <= 0:
        return
    global _slides_last_write_mono
    with _slides_write_lock:
        now = time.monotonic()
        wait = _slides_last_write_mono + interval - now
        if wait > 0:
            time.sleep(wait)


def _mark_slides_write_completed() -> None:
    global _slides_last_write_mono
    with _slides_write_lock:
        _slides_last_write_mono = time.monotonic()


def _http_error_retry_after_seconds(err: HttpError) -> float | None:
    resp = getattr(err, "resp", None)
    if resp is None:
        return None
    raw = None
    if callable(getattr(resp, "get", None)):
        raw = resp.get("retry-after")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _is_slides_write_rate_limit(err: BaseException) -> bool:
    if not isinstance(err, HttpError):
        return False
    status = getattr(err.resp, "status", None)
    if status == 429:
        return True
    # Some client paths surface quota text without a numeric status we trust
    s = str(err).lower()
    return "rate_limit_exceeded" in s or "quota exceeded" in s and "write" in s


def slides_presentations_batch_update(
    slides_service: Any,
    presentation_id: str,
    requests: list[dict[str, Any]],
) -> None:
    """Run ``presentations.batchUpdate`` with per-user write throttling and 429 retries.

    Each API call counts as one write toward ~60/min/user. Hydration and large chunked builds
    otherwise burst past that limit.

    Env (optional):
      BPO_SLIDES_WRITE_INTERVAL_SEC — minimum seconds between successful writes (default 1.05; 0 disables).
      BPO_SLIDES_BATCH_UPDATE_RETRIES — max attempts per call including the first (default 12).
      BPO_SLIDES_BATCH_CHUNK_SIZE — default subrequests per batchUpdate chunk (default 2000, max 5000).
    """
    if not requests:
        return
    max_retries = _slides_batch_update_max_retries()
    last_err: HttpError | None = None
    for attempt in range(max_retries):
        _throttle_before_slides_write()
        try:
            slides_service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={"requests": list(requests)},
            ).execute()
            _mark_slides_write_completed()
            return
        except HttpError as e:
            last_err = e
            if not _is_slides_write_rate_limit(e) or attempt >= max_retries - 1:
                raise
            ra = _http_error_retry_after_seconds(e)
            base = min(120.0, (2**attempt) * 1.0 + random.random())
            delay = max(base, ra) if ra is not None else base
            logger.warning(
                "Slides batchUpdate rate limited (429); sleeping %.1fs then retry %d/%d",
                delay,
                attempt + 2,
                max_retries,
            )
            time.sleep(delay)
    assert last_err is not None
    raise last_err


def _slides_batch_chunk_default() -> int:
    raw = os.environ.get("BPO_SLIDES_BATCH_CHUNK_SIZE", "").strip()
    if not raw:
        return _SLIDES_BATCH_UPDATE_DEFAULT_CHUNK
    try:
        return int(raw)
    except ValueError:
        return _SLIDES_BATCH_UPDATE_DEFAULT_CHUNK


def presentations_batch_update_chunked(
    slides_service: Any,
    presentation_id: str,
    requests: list[dict[str, Any]],
    *,
    chunk_size: int | None = None,
) -> None:
    """Call presentations.batchUpdate in ordered chunks to stay under per-call subrequest limits.

    Env ``BPO_SLIDES_BATCH_CHUNK_SIZE`` sets the default chunk when *chunk_size* is None
    (clamped to ``_SLIDES_BATCH_UPDATE_MAX_CHUNK``). Larger chunks mean fewer round trips
    per deck but each call must stay under API subrequest limits.
    """
    if not requests:
        return
    raw = chunk_size if chunk_size is not None else _slides_batch_chunk_default()
    size = max(1, min(raw, _SLIDES_BATCH_UPDATE_MAX_CHUNK))
    # Never approach the API ceiling (even if a caller passes a huge chunk_size).
    size = min(size, _GOOGLE_SLIDES_MAX_SUBREQUESTS_PER_BATCH // 20)
    n = len(requests)
    if n > size:
        logger.debug(
            "Slides batchUpdate: sending %d subrequest(s) in %d chunk(s) (max %d per call)",
            n,
            (n + size - 1) // size,
            size,
        )
    for i in range(0, n, size):
        chunk = requests[i : i + size]
        slides_presentations_batch_update(slides_service, presentation_id, list(chunk))

