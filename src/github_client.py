"""GitHub REST API — optional preflight and future report enrichment."""

from __future__ import annotations

import requests

from .config import GITHUB_API_BASE_URL, GITHUB_TOKEN, logger


def github_configured() -> bool:
    return bool(GITHUB_TOKEN and str(GITHUB_TOKEN).strip())


def check_github_api() -> tuple[bool, str | None]:
    """Return (True, None) if GitHub is not configured or ``GET /user`` succeeds.

    When ``GITHUB_TOKEN`` is set, failures block deck preflight (same pattern as Salesforce).
    """
    if not github_configured():
        return True, None
    base = (GITHUB_API_BASE_URL or "https://api.github.com").rstrip("/")
    url = f"{base}/user"
    token = str(GITHUB_TOKEN).strip()
    try:
        resp = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=25,
        )
        if resp.status_code == 200:
            return True, None
        detail = (resp.text or "")[:120].replace("\n", " ")
        return False, f"GitHub: HTTP {resp.status_code} {detail}".strip()[:120]
    except Exception as e:
        logger.warning("GitHub preflight failed: %s", e)
        return False, f"GitHub: {str(e)[:120]}"
