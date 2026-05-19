"""Programmatic LeanDNA web-app login (``LDNASESSIONID`` cookie)."""

from __future__ import annotations

import os
import re
import sys
import time
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from .leandna_app_metrics_http import parse_ldna_session_id

DEFAULT_LOGIN_PATH = "/auth/1/authenticate/ldnasession"
DEFAULT_SSO_PATH = "/application/sso.html"
DEFAULT_APP_SERVER = "https://app.staging.leandna.com"
PLAYWRIGHT_INSTALL_HINT = (
    "pip install -r requirements-dev.txt && playwright install chromium"
)


def _log_sso(msg: str) -> None:
    print(f"SSO: {msg}", file=sys.stderr, flush=True)


class LeanDNAAppLoginError(RuntimeError):
    """Could not obtain ``LDNASESSIONID`` from credentials."""


@dataclass(frozen=True)
class LeanDNAAppLoginResult:
    session_id: str
    server: str
    username: str
    http_session: requests.Session


def resolve_login_credentials(
    *,
    username: str | None = None,
    password: str | None = None,
) -> tuple[str, str]:
    """Resolve username/password from args or env (never log password)."""
    user = (
        (username or "").strip()
        or (os.environ.get("LEANDNA_APP_USERNAME") or "").strip()
        or (os.environ.get("LEANDNA_APP_EMAIL") or "").strip()
        or (os.environ.get("JIRA_EMAIL") or "").strip()
    )
    pwd = (password or "").strip() or (os.environ.get("LEANDNA_APP_PASSWORD") or "").strip()
    if not user:
        raise LeanDNAAppLoginError(
            "Missing username — set LEANDNA_APP_USERNAME, LEANDNA_APP_EMAIL, or JIRA_EMAIL "
            "(or pass --username)."
        )
    if not pwd:
        raise LeanDNAAppLoginError(
            "Missing password — set LEANDNA_APP_PASSWORD in .env (your LeanDNA / Atlassian "
            "account password, not the Jira API token). Or pass --password."
        )
    return user, pwd


def login_leandna_app(
    username: str,
    password: str,
    *,
    server: str | None = None,
    login_path: str = DEFAULT_LOGIN_PATH,
    timeout: float = 60.0,
) -> LeanDNAAppLoginResult:
    """POST form credentials to the app auth endpoint and return ``LDNASESSIONID``.

    The live login UI at ``https://app.leandna.com/auth/1/login`` ultimately posts
  ``username`` and ``password`` to ``/auth/1/authenticate/ldnasession``. Metrics API
    calls use ``https://app.leandna.com`` (``LEANDNA_APP_API_SERVER``), not
    ``leandna.atlassian.net`` (that host is Jira Cloud).
    """
    base = (server or os.environ.get("LEANDNA_APP_API_SERVER") or DEFAULT_APP_SERVER).rstrip(
        "/"
    )
    url = f"{base}{login_path}"
    http = requests.Session()
    http.headers.update(
        {
            "User-Agent": "bpo/leandna-app-login/1.0",
            "Accept": "text/html,application/json,*/*",
        }
    )
    resp = http.post(
        url,
        data={"username": username, "password": password},
        allow_redirects=True,
        timeout=timeout,
    )
    sid = _extract_session_id(http, resp)
    if not sid:
        hint = _login_failure_hint(resp)
        raise LeanDNAAppLoginError(
            f"Login did not return LDNASESSIONID (HTTP {resp.status_code}). {hint}"
        )
    return LeanDNAAppLoginResult(
        session_id=sid,
        server=base,
        username=username,
        http_session=http,
    )


def _extract_session_id(session: requests.Session, resp: requests.Response) -> str | None:
    for cookie in session.cookies:
        if cookie.name.upper() == "LDNASESSIONID" and cookie.value:
            return cookie.value.strip()
    header_blob = resp.headers.get("Set-Cookie", "")
    return parse_ldna_session_id(header_blob)


def _login_failure_hint(resp: requests.Response) -> str:
    text = (resp.text or "").lower()
    if "sign in" in text or "login" in text:
        return "Still on login page — check username/password or SSO-only account."
    if resp.status_code == 401:
        return "Unauthorized."
    return "If your org uses SSO-only, copy LDNASESSIONID from the browser instead."


def apply_session_to_env(session_id: str) -> None:
    """Set ``LEANDNA_APP_SESSION_ID`` for the current process (e.g. before metrics calls)."""
    os.environ["LEANDNA_APP_SESSION_ID"] = session_id
    try:
        import src.config as cfg

        cfg.LEANDNA_APP_SESSION_ID = session_id
    except Exception:
        pass


def parse_session_input(raw: str) -> str | None:
    """Parse a pasted ``LDNASESSIONID`` value or full ``Cookie`` header."""
    text = (raw or "").strip()
    if not text:
        return None
    sid = parse_ldna_session_id(text)
    if sid:
        return sid
    if "=" in text or ";" in text:
        return None
    return text


def resolve_app_server(server: str | None = None) -> str:
    return (server or os.environ.get("LEANDNA_APP_API_SERVER") or DEFAULT_APP_SERVER).rstrip(
        "/"
    )


def sso_login_url(server: str | None = None) -> str:
    """URL that starts the LeanDNA web SSO flow."""
    return f"{resolve_app_server(server)}{DEFAULT_SSO_PATH}"


def _session_from_playwright_cookies(cookies: list[dict[str, Any]]) -> str | None:
    for c in cookies:
        name = str(c.get("name") or "")
        if name.upper() == "LDNASESSIONID" and c.get("value"):
            return str(c["value"]).strip()
    return None


def _storage_state_path() -> Path:
    raw = (os.environ.get("BPO_LEANDNA_PLAYWRIGHT_STORAGE_STATE") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parents[1] / ".cache" / "leandna-sso-state.json"


def resolve_sso_email(*, username: str | None = None) -> str:
    """Email for ``POST /auth/1/startSso`` (Google SAML)."""
    user = (
        (username or "").strip()
        or (os.environ.get("LEANDNA_APP_EMAIL") or "").strip()
        or (os.environ.get("LEANDNA_APP_USERNAME") or "").strip()
        or (os.environ.get("JIRA_EMAIL") or "").strip()
    )
    if not user:
        raise LeanDNAAppLoginError(
            "Missing email for automated SSO — set LEANDNA_APP_EMAIL or JIRA_EMAIL."
        )
    return user


def _auto_google_credentials_enabled() -> bool:
    return os.environ.get("BPO_LEANDNA_SSO_AUTO_GOOGLE", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def login_leandna_app_fresh(
    *,
    server: str | None = None,
    timeout: float = 180.0,
    username: str | None = None,
    headless: bool = False,
    interactive: bool = False,
    use_storage_cache: bool = True,
    auto_google_credentials: bool | None = None,
) -> LeanDNAAppLoginResult:
    """Obtain a new ``LDNASESSIONID`` via browser SSO.

    Default: open a browser, submit your LeanDNA email, then **you** complete Google
    sign-in/MFA in that window (does not type your password or trigger laptop MFA push).

    Set ``BPO_LEANDNA_SSO_AUTO_GOOGLE=true`` to auto-fill Google password (may trigger MFA).
    """
    base = resolve_app_server(server)
    email = resolve_sso_email(username=username)
    auto_google = (
        _auto_google_credentials_enabled()
        if auto_google_credentials is None
        else auto_google_credentials
    )
    allow_manual = interactive or (
        os.environ.get("BPO_LEANDNA_SSO_INTERACTIVE", "").strip().lower()
        in ("1", "true", "yes")
    )

    if use_storage_cache:
        _log_sso("Checking cached browser session…")
        cached = _try_storage_state_login(base, email, timeout=timeout, headless=headless)
        if cached is not None:
            _log_sso("Reused cached session.")
            return cached

    pwd = (os.environ.get("LEANDNA_APP_PASSWORD") or "").strip()
    if pwd:
        _log_sso("Trying password POST…")
        try:
            return login_leandna_app(email, pwd, server=base, timeout=min(timeout, 30.0))
        except LeanDNAAppLoginError:
            _log_sso("Password POST failed; continuing with browser SSO…")

    try:
        return _login_sso_playwright_google(
            base,
            email=email,
            password=pwd,
            timeout=timeout,
            headless=headless,
            auto_google_credentials=auto_google,
        )
    except ImportError as e:
        raise LeanDNAAppLoginError(
            f"Playwright required for automated SSO ({e}). {PLAYWRIGHT_INSTALL_HINT}"
        ) from e
    except LeanDNAAppLoginError:
        if allow_manual:
            return _login_sso_manual(
                base,
                timeout=timeout,
                username=email,
                open_browser=True,
                input_stream=None,
            )
        raise


def _try_storage_state_login(
    server: str,
    email: str,
    *,
    timeout: float,
    headless: bool,
) -> LeanDNAAppLoginResult | None:
    path = _storage_state_path()
    if not path.is_file():
        return None
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    deadline = time.monotonic() + min(timeout, 15.0)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(storage_state=str(path))
            page = context.new_page()
            page.goto(
                f"{server}/application/index.html",
                wait_until="domcontentloaded",
                timeout=20_000,
            )
            while time.monotonic() < deadline:
                sid = _session_from_playwright_cookies(context.cookies())
                if sid:
                    return LeanDNAAppLoginResult(
                        session_id=sid,
                        server=server,
                        username=email,
                        http_session=_http_session_with_cookie(server, sid),
                    )
                page.wait_for_timeout(250)
        finally:
            browser.close()
    _log_sso("Cached session expired — removed.")
    path.unlink(missing_ok=True)
    return None


def _save_storage_state(context: Any) -> None:
    path = _storage_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    context.storage_state(path=str(path))


def _google_click_next(page: Any) -> None:
    for sel in ("#identifierNext", "#passwordNext", 'button:has-text("Next")'):
        btn = page.locator(sel)
        if btn.count() and btn.first.is_visible():
            btn.first.click(timeout=10_000)
            return
    page.get_by_role("button", name="Next").click(timeout=10_000)


def _google_on_identifier(page: Any) -> bool:
    url = page.url or ""
    return "signin/identifier" in url or "ServiceLogin" in url


def _google_on_password(page: Any) -> bool:
    url = page.url or ""
    return "signin/challenge/pwd" in url or "password" in url


def _google_run_sign_in_steps(page: Any, email: str, password: str) -> bool:
    """Advance Google login one step. Returns True if something was clicked."""
    if "accounts.google.com" not in (page.url or ""):
        return False

    email_input = page.locator(
        'input[type="email"], input[name="identifier"], #identifierId'
    )
    if _google_on_identifier(page) and email_input.count() and email_input.first.is_visible():
        email_input.first.fill(email, timeout=10_000)
        try:
            with page.expect_navigation(timeout=30_000):
                _google_click_next(page)
        except Exception:
            _google_click_next(page)
        return True

    pwd_input = page.locator('input[type="password"], input[name="Passwd"]')
    if pwd_input.count() and pwd_input.first.is_visible():
        pwd_input.first.fill(password, timeout=10_000)
        try:
            with page.expect_navigation(timeout=45_000):
                _google_click_next(page)
        except Exception:
            _google_click_next(page)
        return True

    totp_secret = (os.environ.get("LEANDNA_APP_TOTP_SECRET") or "").strip()
    if totp_secret:
        try:
            import pyotp
        except ImportError:
            return False
        totp_input = page.locator(
            'input[name="totpPin"], input[name="idvPin"], input[type="tel"]'
        )
        if totp_input.count() and totp_input.first.is_visible():
            totp_input.first.fill(pyotp.TOTP(totp_secret).now(), timeout=10_000)
            try:
                with page.expect_navigation(timeout=30_000):
                    _google_click_next(page)
            except Exception:
                _google_click_next(page)
            return True

    tile = page.get_by_text(email, exact=False)
    if tile.count():
        try:
            with page.expect_navigation(timeout=20_000):
                tile.first.click(timeout=5000)
            return True
        except Exception:
            pass
    return False


def _submit_start_sso(page: Any, server: str, email: str) -> None:
    """Submit the SSO email form and wait for redirect to Google or the app."""
    nav_timeout = 90_000
    known_form = page.locator("#ssoKnownForm")
    if known_form.count() and known_form.is_visible():
        _log_sso("Using known-customer SSO form…")
        with page.expect_navigation(timeout=nav_timeout):
            known_form.locator("button[type='submit']").click(timeout=15_000)
        return

    _log_sso(f"Submitting {email!r} to LeanDNA SSO…")
    email_field = page.locator("#ssoUnknownForm #loginEmail")
    sign_in = page.locator('#ssoUnknownForm button[onclick="continueLogin()"]')
    email_field.fill(email, timeout=15_000)
    with page.expect_navigation(timeout=nav_timeout):
        sign_in.click(timeout=15_000)


def _login_sso_playwright_google(
    server: str,
    *,
    email: str,
    password: str,
    timeout: float,
    headless: bool,
    auto_google_credentials: bool = False,
) -> LeanDNAAppLoginResult:
    from playwright.sync_api import sync_playwright

    entry = sso_login_url(server)
    host_pat = re.escape(urlparse(server).hostname or "leandna.com")
    deadline = time.monotonic() + timeout

    try:
        with sync_playwright() as p:
            _log_sso(f"Launching browser (headless={headless})…")
            browser = p.chromium.launch(headless=headless)
            try:
                context = browser.new_context()
                page = context.new_page()
                _log_sso(f"Opening {entry}")
                page.goto(entry, wait_until="domcontentloaded", timeout=30_000)
                _submit_start_sso(page, server, email)
                _log_sso(f"At {page.url or '(no url)'}")

                last_url = ""
                stuck_since = time.monotonic()
                last_status = 0.0
                google_steps = 0
                max_google_steps = 6
                manual_google_hint = False

                while time.monotonic() < deadline:
                    sid = _session_from_playwright_cookies(context.cookies())
                    if sid:
                        _save_storage_state(context)
                        _log_sso("Got LDNASESSIONID.")
                        return LeanDNAAppLoginResult(
                            session_id=sid,
                            server=server,
                            username=email,
                            http_session=_http_session_with_cookie(server, sid),
                        )

                    cur_url = page.url or ""
                    if cur_url != last_url:
                        last_url = cur_url
                        stuck_since = time.monotonic()
                    stuck_limit = 120.0 if "accounts.google.com" in cur_url else 30.0
                    if time.monotonic() - stuck_since > stuck_limit:
                        short = cur_url[:100] + ("…" if len(cur_url) > 100 else "")
                        raise LeanDNAAppLoginError(
                            f"Sign-in stuck at {short!r} for {stuck_limit:.0f}s. "
                            "Complete MFA in the browser window, or use bin/test-script --interactive."
                        )

                    if "accounts.google.com" in cur_url:
                        if not auto_google_credentials:
                            if not manual_google_hint:
                                _log_sso(
                                    "Complete Google sign-in and MFA in the browser window "
                                    "(password is not entered automatically)."
                                )
                                manual_google_hint = True
                        elif google_steps < max_google_steps:
                            if _google_run_sign_in_steps(page, email, password):
                                google_steps += 1
                                u = page.url or ""
                                _log_sso(
                                    f"Google step {google_steps}/{max_google_steps} → "
                                    f"{u[:90]}{'…' if len(u) > 90 else ''}"
                                )
                        page.wait_for_timeout(800)
                    else:
                        try:
                            page.wait_for_url(
                                re.compile(rf"https?://{host_pat}/"),
                                timeout=3_000,
                            )
                        except Exception:
                            page.wait_for_timeout(500)

                    now = time.monotonic()
                    if now - last_status >= 10.0:
                        _log_sso(f"Waiting… {cur_url[:100]}")
                        last_status = now

                raise LeanDNAAppLoginError(
                    f"Timed out after {timeout:.0f}s waiting for LDNASESSIONID after Google SSO. "
                    "Try: bin/test-script --headed  (or --interactive to paste a cookie)."
                )
            finally:
                browser.close()
    except LeanDNAAppLoginError:
        raise
    except Exception as e:
        raise LeanDNAAppLoginError(f"SSO browser automation failed: {e}") from e


def _http_session_with_cookie(server: str, session_id: str) -> requests.Session:
    http = requests.Session()
    http.headers.update(
        {
            "User-Agent": "bpo/leandna-app-login/1.0",
            "Accept": "text/html,application/json,*/*",
        }
    )
    host = urlparse(server).hostname or "app.staging.leandna.com"
    http.cookies.set("LDNASESSIONID", session_id, domain=host)
    return http


def login_leandna_app_sso(
    *,
    server: str | None = None,
    timeout: float = 300.0,
    username: str | None = None,
    use_playwright: bool = True,
    open_browser: bool = True,
    input_stream: Any | None = None,
) -> LeanDNAAppLoginResult:
    """Obtain ``LDNASESSIONID`` via SSO (automated by default)."""
    if input_stream is not None or (not use_playwright and open_browser):
        base = resolve_app_server(server)
        label = (username or "").strip() or _optional_username_for_display()
        return _login_sso_manual(
            base,
            timeout=timeout,
            username=label,
            open_browser=open_browser,
            input_stream=input_stream,
        )
    return login_leandna_app_fresh(
        server=server,
        timeout=timeout,
        username=username,
        headless=True,
        interactive=False,
    )


def _optional_username_for_display() -> str:
    for key in ("LEANDNA_APP_EMAIL", "LEANDNA_APP_USERNAME", "JIRA_EMAIL"):
        v = (os.environ.get(key) or "").strip()
        if v:
            return v
    return ""


def _login_sso_manual(
    server: str,
    *,
    timeout: float,
    username: str,
    open_browser: bool,
    input_stream: Any | None,
) -> LeanDNAAppLoginResult:
    entry = sso_login_url(server)
    if open_browser:
        webbrowser.open(entry)
    print(
        "LeanDNA uses SSO — complete sign-in in your browser, then paste the session cookie.\n"
        f"  1. Open (if needed): {entry}\n"
        "  2. DevTools → Application → Cookies → app.staging.leandna.com → LDNASESSIONID\n"
        "  3. Paste the value (or full Cookie header) below and press Enter:",
        file=sys.stderr,
    )
    stream = input_stream if input_stream is not None else sys.stdin
    if input_stream is None and not stream.isatty():
        raise LeanDNAAppLoginError(
            "SSO login needs an interactive terminal to paste LDNASESSIONID, or set "
            "LEANDNA_APP_SESSION_ID in .env. Install playwright for automated browser login: "
            "pip install playwright && playwright install chromium"
        )
    try:
        line = stream.readline()
    except Exception as e:
        raise LeanDNAAppLoginError(f"Could not read session from stdin: {e}") from e
    sid = parse_session_input(line)
    if not sid:
        raise LeanDNAAppLoginError(
            "No LDNASESSIONID in pasted input — paste the cookie value or "
            "LDNASESSIONID=… from DevTools."
        )
    return LeanDNAAppLoginResult(
        session_id=sid,
        server=server,
        username=username,
        http_session=_http_session_with_cookie(server, sid),
    )
