"""Preflight checks for required data sources before running deck generation.

If Pendo, Salesforce (when configured), GitHub (when configured), or CS Report is down,
deck runs abort with a clear error instead of proceeding with partial data.
"""

from __future__ import annotations

from pathlib import Path

from .config import (
    JIRA_API_TOKEN,
    JIRA_URL,
    PENDO_INTEGRATION_KEY,
    SF_LOGIN_URL,
    SF_CONSUMER_KEY,
    SF_USERNAME,
    SF_PRIVATE_KEY,
    SF_PRIVATE_KEY_PATH,
    logger,
)


def check_pendo() -> tuple[bool, str | None]:
    """Return (True, None) if Pendo is up, else (False, error_message)."""
    if not PENDO_INTEGRATION_KEY:
        return False, "Pendo: PENDO_INTEGRATION_KEY is not set"
    try:
        from .pendo_aggregate import call_with_pendo_retry
        from .pendo_client import PendoClient

        client = PendoClient()
        call_with_pendo_retry(
            lambda: client.get_sites_by_customer(days=1),
            label="preflight",
            max_attempts=3,
        )
        return True, None
    except Exception as e:
        logger.warning("Pendo preflight failed: %s", e)
        return False, f"Pendo: {str(e)[:120]}"


def _salesforce_private_key_available() -> bool:
    """True when JWT auth material is present (inline PEM or readable key file)."""
    if SF_PRIVATE_KEY and str(SF_PRIVATE_KEY).strip():
        return True
    if SF_PRIVATE_KEY_PATH:
        path = Path(SF_PRIVATE_KEY_PATH).expanduser()
        if path.is_file():
            return True
        logger.warning("SF_PRIVATE_KEY_PATH file not found: %s", path)
    return False


def _salesforce_configured() -> bool:
    """True when Salesforce JWT credentials are complete and usable."""
    return bool(
        SF_LOGIN_URL
        and SF_CONSUMER_KEY
        and SF_USERNAME
        and _salesforce_private_key_available()
    )


def check_salesforce() -> tuple[bool, str | None]:
    """Return (True, None) if Salesforce is not configured or auth + query succeed.

    When credentials are set, failures block deck generation (same as Pendo / CS Report).
    """
    if not _salesforce_configured():
        return True, None
    try:
        from .salesforce_client import SalesforceClient
        client = SalesforceClient()
        client._ensure_token()
        client._query("SELECT Id FROM Account LIMIT 1")
        return True, None
    except Exception as e:
        logger.warning("Salesforce preflight failed: %s", e)
        return False, f"Salesforce: {str(e)[:120]}"


def check_cs_report() -> tuple[bool, str | None]:
    """Return (True, None) if CS Report (Drive + Data Exports) is reachable; else (False, error_message)."""
    try:
        from .cs_report_client import check_reachable
        check_reachable()
        return True, None
    except Exception as e:
        logger.warning("CS Report preflight failed: %s", e)
        return False, f"CS Report: {str(e)[:120]}"


def check_github() -> tuple[bool, str | None]:
    """Return (True, None) if GitHub is not configured or the API accepts the token."""
    try:
        from .github_client import check_github_api
        return check_github_api()
    except Exception as e:
        logger.warning("GitHub preflight failed: %s", e)
        return False, f"GitHub: {str(e)[:120]}"


def check_slack() -> tuple[bool, str | None]:
    """Return (True, None) if Slack is not configured or ``auth.test`` succeeds."""
    try:
        from .slack_client import check_slack_api
        return check_slack_api()
    except Exception as e:
        logger.warning("Slack preflight failed: %s", e)
        return False, f"Slack: {str(e)[:120]}"


def _jira_configured() -> bool:
    return bool(JIRA_URL and JIRA_API_TOKEN)


def check_jira() -> tuple[bool, str | None]:
    """Return (True, None) if Jira credentials are set and ``/rest/api/3/myself`` succeeds."""
    if not _jira_configured():
        return False, "Jira: JIRA_URL and JIRA_API_TOKEN must be set"
    try:
        import requests

        from .jira_client import get_shared_jira_client

        client = get_shared_jira_client()
        resp = requests.get(
            f"{client.api_base_url}/rest/api/3/myself",
            headers=client._headers,
            timeout=30,
        )
        resp.raise_for_status()
        return True, None
    except Exception as e:
        logger.warning("Jira preflight failed: %s", e)
        return False, f"Jira: {str(e)[:120]}"


_PREFLIGHT_SOURCES_DEFAULT = ("pendo", "salesforce", "github", "slack", "cs_report")
_JIRA_BACKED_DECK_PREFLIGHT_SOURCES = ("jira", "github")


def _run_preflight(source: str) -> tuple[bool, str | None]:
    if source == "pendo":
        return check_pendo()
    if source == "salesforce":
        return check_salesforce()
    if source == "github":
        return check_github()
    if source == "slack":
        return check_slack()
    if source == "cs_report":
        return check_cs_report()
    if source == "jira":
        return check_jira()
    raise ValueError(f"unknown preflight source {source!r}")


def check_all_required(*, sources: tuple[str, ...] | None = None) -> list[str]:
    """Run preflight on the requested integrations.

    Default: Pendo, Salesforce (if configured), GitHub/Slack (if configured), CS Report.
    Pass ``sources`` to scope checks (e.g. Jira-backed decks use ``jira`` + ``github`` only).
    """
    errors: list[str] = []
    for key in sources or _PREFLIGHT_SOURCES_DEFAULT:
        ok, msg = _run_preflight(key)
        if not ok and msg:
            errors.append(msg)
    return errors


def check_jira_backed_deck_required() -> list[str]:
    """Preflight for engineering-portfolio and implementations_review (Jira primary; GitHub optional)."""
    return check_all_required(sources=_JIRA_BACKED_DECK_PREFLIGHT_SOURCES)


def integration_freshness_metadata() -> dict[str, object]:
    """Integration configuration and cache freshness for run summaries / unattended gates."""
    from .config import (
        CORTEX_SALESFORCE_CACHE_TTL_SECONDS,
        CORTEX_SLACK_CACHE_TTL_SECONDS,
        CURSOR_ADMIN_API_KEY,
        GITHUB_TOKEN,
        SF_CONSUMER_KEY,
        SF_LOGIN_URL,
        SF_USERNAME,
        SLACK_BOT_TOKEN,
    )
    from .salesforce_client import salesforce_read_cache_age_hours

    sf_configured = _salesforce_configured()
    meta: dict[str, object] = {
        "github_configured": bool(GITHUB_TOKEN),
        "cursor_configured": bool(CURSOR_ADMIN_API_KEY),
        "ai_productivity_configured": bool(GITHUB_TOKEN and CURSOR_ADMIN_API_KEY),
        "salesforce_configured": sf_configured,
        "salesforce_cache_ttl_h": round(CORTEX_SALESFORCE_CACHE_TTL_SECONDS / 3600.0, 2),
        "slack_configured": bool(SLACK_BOT_TOKEN),
        "slack_cache_ttl_h": round(CORTEX_SLACK_CACHE_TTL_SECONDS / 3600.0, 2),
    }
    age_h = salesforce_read_cache_age_hours()
    if age_h is not None:
        meta["salesforce_cache_age_h"] = round(float(age_h), 2)
    return meta
