#!/usr/bin/env python3
"""Log into LeanDNA app, print ``LDNASESSIONID``, and call Metrics/View (like metrics-get-mine).

Opens a browser for LeanDNA SSO, submits ``LEANDNA_APP_EMAIL``, then you finish Google
sign-in/MFA in that window (does not auto-type your password or ping laptop MFA by default).
Reuses ``.cache/leandna-sso-state.json`` when still valid.

Install once: ``pip install -r requirements-dev.txt && playwright install chromium``

Examples::

  test-script
  test-script --format brief
  test-script
  test-script --headless
  test-script --interactive
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")
# Never reuse a cached app session from .env for this script.
for _key in ("LEANDNA_APP_SESSION_ID", "LEANDNA_APP_COOKIE", "LEANDNA_DATA_API_COOKIE"):
    os.environ.pop(_key, None)
import requests  # noqa: E402

from src.config import LEANDNA_APP_API_SERVER, LEANDNA_APP_FACTORY_NDX  # noqa: E402
from src.leandna_app_login import (  # noqa: E402
    LeanDNAAppLoginError,
    apply_session_to_env,
    login_leandna_app,
    login_leandna_app_fresh,
    resolve_login_credentials,
)
from src.leandna_app_metrics_client import (  # noqa: E402
    list_my_metrics_view,
    metric_view_label,
    resolve_app_metric_owner,
)
from src.leandna_app_metrics_http import LeanDNAAppSessionError  # noqa: E402


def _sort_rows(rows: list[dict]) -> list[dict]:
    def key(r: dict) -> tuple:
        raw = r.get("ndx", r.get("id"))
        try:
            return (0, int(raw))
        except (TypeError, ValueError):
            return (1, str(raw or ""))

    return sorted(rows, key=key)


def _brief_lines(rows: list[dict]) -> list[str]:
    lines = []
    for r in rows:
        mid = r.get("ndx", r.get("id", ""))
        name = metric_view_label(r).replace("\t", " ").replace("\n", " ")
        mtype = str(r.get("metricType") or "")
        vs = r.get("valueStreamNdx", "")
        owner = r.get("metricOwner", "")
        lines.append(f"{mid}\t{name}\t{mtype}\tvalueStreamNdx={vs}\tmetricOwner={owner}")
    return lines


def _print_session(sid: str, *, show_session: bool) -> None:
    if show_session:
        print(f"LDNASESSIONID={sid}")
    else:
        masked = f"{sid[:4]}…{sid[-4:]}" if len(sid) > 10 else sid
        print(f"LDNASESSIONID={masked}  (use --show-session for full value)")
    print(f"Set in .env: LEANDNA_APP_SESSION_ID={sid}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Automated SSO login to LeanDNA app, then Metrics/View.",
    )
    ap.add_argument(
        "--login-password",
        action="store_true",
        help="POST username/password instead of Google SSO (rare)",
    )
    ap.add_argument(
        "--headless",
        action="store_true",
        help="Hide browser during SSO (default: visible window for MFA)",
    )
    ap.add_argument(
        "--auto-google",
        action="store_true",
        help="Auto-type Google password (may trigger laptop MFA push; default is manual)",
    )
    ap.add_argument(
        "--interactive",
        action="store_true",
        help="Fall back to manual browser + paste cookie if automation fails",
    )
    ap.add_argument("--username", default=None, help="Override LEANDNA_APP_EMAIL / JIRA_EMAIL")
    ap.add_argument("--password", default=None, help="Override LEANDNA_APP_PASSWORD")
    ap.add_argument(
        "--server",
        default=None,
        help=f"App host (default: LEANDNA_APP_API_SERVER or {LEANDNA_APP_API_SERVER})",
    )
    ap.add_argument("--format", choices=("json", "brief"), default="json")
    ap.add_argument("--view-query", default=None)
    ap.add_argument("--factory-ndx", type=int, default=None)
    ap.add_argument("--metric-owner", default=None, metavar="NAME")
    ap.add_argument("--no-switch-site", action="store_true")
    ap.add_argument(
        "--show-session",
        action="store_true",
        help="Print full LDNASESSIONID (default: first/last 4 chars only)",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        metavar="SEC",
        help="SSO timeout (default 120)",
    )
    ns = ap.parse_args()

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    server = (ns.server or LEANDNA_APP_API_SERVER or "https://app.staging.leandna.com").strip()
    print(f"App server: {server}", file=sys.stderr)

    if ns.login_password:
        try:
            user, pwd = resolve_login_credentials(
                username=ns.username, password=ns.password
            )
        except LeanDNAAppLoginError as e:
            print(str(e), file=sys.stderr)
            return 1
        print(f"Password login as {user!r}…", file=sys.stderr)
        try:
            result = login_leandna_app(user, pwd, server=server, timeout=ns.timeout)
        except LeanDNAAppLoginError as e:
            print(f"Login failed: {e}", file=sys.stderr)
            return 1
        apply_session_to_env(result.session_id)
        _print_session(result.session_id, show_session=ns.show_session)
    else:
        print("Browser SSO login…", file=sys.stderr)
        try:
            result = login_leandna_app_fresh(
                server=server,
                timeout=ns.timeout,
                username=ns.username,
                headless=ns.headless,
                interactive=ns.interactive,
                use_storage_cache=True,
                auto_google_credentials=ns.auto_google,
            )
        except LeanDNAAppLoginError as e:
            print(f"Login failed: {e}", file=sys.stderr)
            return 1
        apply_session_to_env(result.session_id)
        _print_session(result.session_id, show_session=ns.show_session)

    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX
    owner, _identity, kind = resolve_app_metric_owner(
        factory_ndx=factory,
        metric_owner=ns.metric_owner,
        timeout=ns.timeout,
    )
    if not owner:
        print(
            "Could not determine metricOwner — set LEANDNA_APP_METRIC_OWNER or run with "
            "a session that can load Metrics/View.",
            file=sys.stderr,
        )
        return 1

    print(
        f"GET …/factndx/{factory}/Metrics/View?metricOwner={owner!r} ({kind})",
        file=sys.stderr,
    )

    try:
        rows = list_my_metrics_view(
            owner,
            owner_kind=kind,
            view_query=ns.view_query,
            factory_ndx=factory,
            switch_site_first=not ns.no_switch_site,
            timeout=ns.timeout,
        )
    except LeanDNAAppSessionError as e:
        print(str(e), file=sys.stderr)
        return 1
    except requests.HTTPError as e:
        print(f"Metrics/View failed: {e}", file=sys.stderr)
        return 1
    except ConnectionError as e:
        print(f"Metrics/View failed: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Metrics/View failed: {e}", file=sys.stderr)
        return 1

    slim = _sort_rows(rows)
    if ns.format == "json":
        print(json.dumps(slim, indent=2, default=str, ensure_ascii=False))
    else:
        print("\t".join(["ndx", "name", "metricType", "valueStreamNdx", "metricOwner"]))
        for ln in _brief_lines(slim):
            print(ln)

    print(f"Displayed {len(slim)} metric(s).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
