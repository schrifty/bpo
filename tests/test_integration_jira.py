"""Live read-only checks against Jira Cloud via the Atlassian API gateway.

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
    cloud = (os.environ.get("JIRA_CLOUD_ID") or "").strip()
    auto = (os.environ.get("JIRA_CLOUD_ID_AUTO") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    return bool(token and url and (cloud or auto))


@pytest.mark.jira_live
def test_jira_gateway_live_myself_and_help_count(capsys) -> None:
    """Gateway auth + ``GET /myself`` + HELP approximate-count (read-only smoke)."""
    _ensure_verbose_logging()
    _LOG.info("=== Jira gateway live read: begin ===")

    if not _jira_live_credentials_configured():
        pytest.skip(
            "Jira credentials missing — set JIRA_API_TOKEN, JIRA_URL, and "
            "JIRA_CLOUD_ID or JIRA_CLOUD_ID_AUTO=true"
        )

    jc_mod, jclient_mod = _load_jira_modules()

    try:
        settings = jc_mod.build_jira_connection_settings()
    except (ValueError, requests.HTTPError) as e:
        pytest.fail(
            f"Jira connection settings failed: {e}. "
            "Set JIRA_CLOUD_ID explicitly or fix JIRA_API_TOKEN / JIRA_SERVICE_ACCOUNT_AUTH."
        )

    summary = jc_mod.jira_connection_summary(settings)
    _LOG.info(
        "connection auth_mode=%s api_base=%s browse=%s cloud_id=%s…",
        summary.get("auth_mode"),
        summary.get("api_base_url"),
        summary.get("browse_base_url"),
        (settings.cloud_id or "")[:8],
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
        f"Jira OK: {display or account_id} @ {summary.get('browse_base_url')} "
        f"(cloud …{settings.cloud_id[-6:]}, HELP ~{help_count} issues updated last 1d)"
    )
    _LOG.info("%s", line)

    with capsys.disabled():
        sys.stdout.write("\n--- Jira live read (gateway) ---\n")
        sys.stdout.write(f"{line}\n")
        sys.stdout.write("--------------------------------\n")
        sys.stdout.flush()

    assert help_count >= 0
    _LOG.info("=== Jira gateway live read: success ===")
