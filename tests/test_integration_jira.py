"""Live read-only checks against Jira Cloud (site REST by default; gateway if configured).

Tests **skip** when ``JIRA_API_TOKEN`` or ``JIRA_URL`` are missing. Load credentials from
``.env`` (``override=True`` so on-disk values win over stale shell exports) and run::

    python3 -m pytest tests/test_integration_jira.py -v

Does **not** log tokens or ``Authorization`` headers.
"""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

import pytest
import requests

_ROOT = Path(__file__).resolve().parents[1]
_LOG = logging.getLogger("integration_jira")


def _ensure_verbose_logging() -> None:
    fmt = "%(asctime)s %(levelname)-5s [%(name)s] %(message)s"
    root = logging.getLogger()
    if not any(type(h) is logging.StreamHandler for h in root.handlers):
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(fmt))
        root.addHandler(h)
    root.setLevel(logging.INFO)
    logging.getLogger("integration_jira").setLevel(logging.DEBUG)


def _load_jira_modules():
    try:
        from dotenv import load_dotenv
    except ImportError:
        pytest.skip("python-dotenv not installed")
    load_dotenv(_ROOT / ".env", override=True)

    import src.config as _config

    importlib.reload(_config)

    import src.jira_connection as _jc
    import src.jira_client as _jclient

    importlib.reload(_jc)
    importlib.reload(_jclient)
    _jclient.reset_shared_jira_client()
    return _jc, _jclient


def _jira_live_credentials_configured() -> bool:
    import os

    token = (os.environ.get("JIRA_API_TOKEN") or "").strip()
    url = (os.environ.get("JIRA_URL") or "").strip()
    if not (token and url):
        return False
    mode = (os.environ.get("JIRA_AUTH_MODE") or "site").strip().lower()
    if mode in ("gateway", "atlassian", "cloud"):
        cloud = (os.environ.get("JIRA_CLOUD_ID") or "").strip()
        auto = (os.environ.get("JIRA_CLOUD_ID_AUTO") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        return bool(cloud or auto)
    return bool((os.environ.get("JIRA_EMAIL") or "").strip())


@pytest.mark.jira_live
def test_jira_gateway_live_myself_and_help_count(capsys) -> None:
    """Live ``GET /myself`` + HELP approximate-count (site or gateway from .env)."""
    _ensure_verbose_logging()
    _LOG.info("=== Jira live read: begin ===")

    if not _jira_live_credentials_configured():
        pytest.skip(
            "Jira credentials missing — site (default): JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN; "
            "gateway: JIRA_AUTH_MODE=gateway plus JIRA_CLOUD_ID or JIRA_CLOUD_ID_AUTO=true"
        )

    jc_mod, jclient_mod = _load_jira_modules()

    try:
        settings = jc_mod.build_jira_connection_settings()
    except (ValueError, requests.HTTPError) as e:
        pytest.fail(
            f"Jira connection settings failed: {e}. "
            "Site: JIRA_URL + JIRA_EMAIL + JIRA_API_TOKEN. "
            "Gateway: JIRA_AUTH_MODE=gateway + JIRA_CLOUD_ID (or AUTO) + token."
        )

    summary = jc_mod.jira_connection_summary(settings)
    _LOG.info(
        "connection auth_mode=%s api_base=%s browse=%s%s",
        summary.get("auth_mode"),
        summary.get("api_base_url"),
        summary.get("browse_base_url"),
        f" cloud_id={settings.cloud_id[:8]}…" if settings.cloud_id else "",
    )

    api = settings.api_base_url.rstrip("/")
    headers = dict(settings.headers)

    myself_url = f"{api}/rest/api/3/myself"
    try:
        myself_resp = requests.get(myself_url, headers=headers, timeout=30)
        myself_resp.raise_for_status()
    except requests.RequestException as e:
        pytest.fail(f"GET /rest/api/3/myself failed: {e}")

    myself = myself_resp.json()
    assert isinstance(myself, dict)
    account_id = (myself.get("accountId") or "").strip()
    display = (myself.get("displayName") or myself.get("name") or "").strip()
    assert account_id or display, f"/myself returned no identity fields: {list(myself.keys())}"

    jira = jclient_mod.get_shared_jira_client()
    help_jql = 'project = HELP AND updated >= -1d'
    help_count = jira._jql_match_total(help_jql)
    if help_count is None:
        pytest.fail(
            "HELP approximate-count returned no count — token may lack Jira issue read scope "
            f"(JQL: {help_jql!r})"
        )

    line = (
        f"Jira OK: {display or account_id} @ {summary.get('api_base_url')} "
        f"({settings.auth_mode}{', cloud …' + settings.cloud_id[-6:] if settings.cloud_id else ''}"
        f", HELP ~{help_count} issues updated last 1d)"
    )
    _LOG.info("%s", line)

    with capsys.disabled():
        sys.stdout.write(f"\n--- Jira live read ({settings.auth_mode}) ---\n")
        sys.stdout.write(f"{line}\n")
        sys.stdout.write("--------------------------------\n")
        sys.stdout.flush()

    assert help_count >= 0
    _LOG.info("=== Jira live read: success ===")
