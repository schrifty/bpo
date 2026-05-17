#!/usr/bin/env python3
"""List LeanDNA metrics assigned to the **current user** (classic app API).

Uses ``metricOwner`` on ``GET …/Metrics/View`` (usually your **display name**, e.g.
``Marc Schriftman`` — not the numeric kpi ``userNdx``).

Set ``LEANDNA_APP_METRIC_OWNER`` in ``.env``, or run ``whoami`` to see owner labels.

Examples::

  python3 scripts/get-my-metrics.py
  python3 scripts/get-my-metrics.py --format brief
  python3 scripts/get-my-metrics.py --metric-owner "Marc Schriftman"
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

import requests  # noqa: E402

from src.config import (  # noqa: E402
    LEANDNA_APP_API_SERVER,
    LEANDNA_APP_FACTORY_NDX,
    LEANDNA_APP_USER_NDX,
)
from src.leandna_app_metrics_client import (  # noqa: E402
    identity_display_name,
    list_my_metrics_view,
    metric_view_label,
    resolve_app_metric_owner,
)
from src.leandna_app_metrics_http import leandna_app_session_configured  # noqa: E402


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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List metrics owned by you (app API Metrics/View metricOwner filter).",
    )
    ap.add_argument("--format", choices=("json", "brief"), default="json")
    ap.add_argument("--view-query", default=None)
    ap.add_argument("--factory-ndx", type=int, default=None)
    ap.add_argument(
        "--metric-owner",
        default=None,
        metavar="NAME",
        help='Your metricOwner label (e.g. "Marc Schriftman")',
    )
    ap.add_argument(
        "--user-ndx",
        default=None,
        metavar="NDX",
        help="Numeric user ndx (often wrong for Metrics/View — prefer --metric-owner)",
    )
    ap.add_argument("--no-switch-site", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--timeout", type=float, default=60.0, metavar="SEC")
    ns = ap.parse_args()

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    if not leandna_app_session_configured():
        print("Missing LEANDNA_APP_SESSION_ID or LDNASESSIONID cookie.", file=sys.stderr)
        return 1

    factory = ns.factory_ndx if ns.factory_ndx is not None else LEANDNA_APP_FACTORY_NDX

    owner, identity, kind = resolve_app_metric_owner(
        factory_ndx=factory,
        metric_owner=ns.metric_owner,
        user_ndx=ns.user_ndx,
        timeout=ns.timeout,
    )

    if not owner:
        print(
            "Could not determine metric owner — set LEANDNA_APP_METRIC_OWNER in .env "
            '(your name as shown in the app, e.g. "Marc Schriftman") or run whoami.',
            file=sys.stderr,
        )
        return 1

    if kind == "ndx" and LEANDNA_APP_USER_NDX and str(owner) == LEANDNA_APP_USER_NDX:
        print(
            f"Warning: using numeric LEANDNA_APP_USER_NDX={owner!r}. "
            "App Metrics/View usually expects metricOwner as your display name. "
            "Run whoami or set LEANDNA_APP_METRIC_OWNER.",
            file=sys.stderr,
        )

    print(
        f"LeanDNA app API: GET …/factndx/{factory}/Metrics/View?metricOwner={owner!r} ({kind})",
        file=sys.stderr,
    )
    if identity:
        label = identity_display_name(identity)
        if label:
            print(f"Identity: {label}", file=sys.stderr)

    try:
        rows = list_my_metrics_view(
            owner,
            owner_kind=kind,
            view_query=ns.view_query,
            factory_ndx=factory,
            switch_site_first=not ns.no_switch_site,
            timeout=ns.timeout,
        )
    except requests.HTTPError as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    rows = _sort_rows(rows)
    if ns.format == "brief":
        print("ndx\tname\tmetricType\tvalueStreamNdx\tmetricOwner")
        for ln in _brief_lines(rows):
            print(ln)
    else:
        payload: Any = {"metricOwner": owner, "ownerFilterKind": kind, "metrics": rows}
        if identity:
            payload["identity"] = identity
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))

    print(f"Displayed {len(rows)} metric(s) for metricOwner={owner!r}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
