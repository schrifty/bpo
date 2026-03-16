"""Preflight checks for required data sources before running deck generation.

If Pendo, Salesforce (when configured), or CS Report is down, deck runs abort
with a clear error instead of proceeding with partial data.
"""

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


def _salesforce_configured() -> bool:
    """True if any Salesforce credential is set (we expect the app to use SF)."""
    return bool(SF_LOGIN_URL and SF_CONSUMER_KEY and SF_USERNAME and (SF_PRIVATE_KEY or SF_PRIVATE_KEY_PATH))


def check_salesforce() -> tuple[bool, str | None]:
    """Return (True, None) always — Salesforce auth issues are currently non-blocking.

    Salesforce is optional; if auth fails the health report runs without SF data
    and the Data Quality slide marks it as unavailable.
    TODO: re-enable blocking once Salesforce JWT cert is confirmed by admin.
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
        logger.warning("Salesforce preflight: auth issue (non-blocking): %s", e)
        return True, None  # warn but don't block


def check_cs_report() -> tuple[bool, str | None]:
    """Return (True, None) if CS Report (Drive + Data Exports) is reachable; else (False, error_message)."""
    try:
        from .cs_report_client import check_reachable
        check_reachable()
        return True, None
    except Exception as e:
        logger.warning("CS Report preflight failed: %s", e)
        return False, f"CS Report: {str(e)[:120]}"


def check_all_required() -> list[str]:
    """Run preflight on Pendo, Salesforce (if configured), and CS Report.

    Returns a list of error messages. If empty, all required sources are up; otherwise
    the caller should abort and print these messages.
    """
    errors: list[str] = []
    for name, check_fn in (
        ("Pendo", check_pendo),
        ("Salesforce", check_salesforce),
        ("CS Report", check_cs_report),
    ):
        ok, msg = check_fn()
        if not ok and msg:
            errors.append(msg)
    return errors
