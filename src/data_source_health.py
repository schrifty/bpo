"""Preflight checks for required data sources before running deck generation.

If Pendo, Salesforce (when configured), GitHub (when configured), or CS Report is down,
deck runs abort with a clear error instead of proceeding with partial data.
"""

from __future__ import annotations

from pathlib import Path

from .config import (
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
        from .pendo_client import PendoClient
        client = PendoClient()
        # Minimal API call to confirm we can reach Pendo
        client.get_sites_by_customer(days=1)
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


def check_all_required() -> list[str]:
    """Run preflight on Pendo, Salesforce (if configured), GitHub/Slack (if configured), and CS Report.

    Returns a list of error messages. If empty, all required sources are up; otherwise
    the caller should abort and print these messages.
    """
    errors: list[str] = []
    for name, check_fn in (
        ("Pendo", check_pendo),
        ("Salesforce", check_salesforce),
        ("GitHub", check_github),
        ("Slack", check_slack),
        ("CS Report", check_cs_report),
    ):
        ok, msg = check_fn()
        if not ok and msg:
            errors.append(msg)
    return errors


def integration_freshness_metadata() -> dict[str, object]:
    """Integration configuration and cache freshness for run summaries / unattended gates."""
    from .config import (
        CORTEX_SALESFORCE_CACHE_TTL_SECONDS,
        CURSOR_ADMIN_API_KEY,
        GITHUB_TOKEN,
        SF_CONSUMER_KEY,
        SF_LOGIN_URL,
        SF_USERNAME,
    )
    from .salesforce_client import salesforce_read_cache_age_hours

    sf_configured = _salesforce_configured()
    meta: dict[str, object] = {
        "github_configured": bool(GITHUB_TOKEN),
        "cursor_configured": bool(CURSOR_ADMIN_API_KEY),
        "ai_productivity_configured": bool(GITHUB_TOKEN and CURSOR_ADMIN_API_KEY),
        "salesforce_configured": sf_configured,
        "salesforce_cache_ttl_h": round(CORTEX_SALESFORCE_CACHE_TTL_SECONDS / 3600.0, 2),
    }
    age_h = salesforce_read_cache_age_hours()
    if age_h is not None:
        meta["salesforce_cache_age_h"] = round(float(age_h), 2)
    return meta
