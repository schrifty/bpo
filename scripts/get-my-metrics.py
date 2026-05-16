#!/usr/bin/env python3
"""List LeanDNA metric definitions owned by the current API user.

Calls ``GET /data/identity`` for ``userId``, then ``GET /data/Metric`` and keeps rows
whose ``ownerId`` matches that user (same auth / ``EXECUTION_ENV`` rules as ``get-metrics.py``).

Examples::

  python3 scripts/get-my-metrics.py
  python3 scripts/get-my-metrics.py --format brief
  python3 scripts/get-my-metrics.py --requested-sites 416
  python3 scripts/get-my-metrics.py --user-id 75321
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

from src.config import BPO_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.leandna_data_api_http import leandna_data_api_credentials_configured  # noqa: E402
from src.leandna_data_api_request import data_api_base_url, data_api_get_json  # noqa: E402
from src.leandna_metrics_client import list_metric_definitions  # noqa: E402


def _id_matches(raw_id: Any, want_s: str) -> bool:
    w = (want_s or "").strip()
    if not w:
        return False
    try:
        return int(raw_id) == int(w)
    except (TypeError, ValueError):
        return str(raw_id).strip() == w


def _sort_rows(rows: list[dict]) -> list[dict]:
    def key(r: dict) -> tuple:
        raw = r.get("id")
        try:
            return (0, int(raw))
        except (TypeError, ValueError):
            return (1, str(raw or ""))

    return sorted(rows, key=key)


def _brief_lines(rows: list[dict]) -> list[str]:
    lines = []
    for r in rows:
        mid = r.get("id", "")
        name = str(r.get("name") or r.get("crossSiteName") or "").replace("\t", " ").replace("\n", " ")
        mtype = str(r.get("metricType") or "")
        site = str(r.get("siteId") or "")
        owner = str(r.get("ownerId") or "")
        cats = r.get("currentCategories")
        cat_s = ""
        if isinstance(cats, list):
            cat_s = ",".join(str(x) for x in cats[:8])
            if len(cats) > 8:
                cat_s += ",…"
        elif cats is not None:
            cat_s = str(cats)
        vstreams = r.get("possibleValueStreams")
        vs_s = ""
        if isinstance(vstreams, list):
            vs_s = ",".join(str(x.get("id", x) if isinstance(x, dict) else x) for x in vstreams[:5])
            if len(vstreams) > 5:
                vs_s += ",…"
        lines.append(
            f"{mid}\t{name}\t{mtype}\tsiteId={site}\townerId={owner}\tcategories={cat_s}\tvalueStreams={vs_s}"
        )
    return lines


def _identity_user_id(body: Any) -> str | None:
    if not isinstance(body, dict):
        return None
    raw = body.get("userId")
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _identity_display_name(body: dict[str, Any]) -> str:
    for key in ("userName", "emailAddress"):
        v = body.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _fetch_identity(
    *,
    requested_sites: str | None,
    read_timeout_seconds: float,
) -> dict[str, Any]:
    env = data_api_get_json(
        "identity",
        requested_sites=requested_sites,
        timeout_seconds=read_timeout_seconds,
        user_agent_suffix="get-my-metrics/1.0",
    )
    if not env.get("ok"):
        raise RuntimeError(f"GET /data/identity failed: {env!r}")
    body = env.get("body")
    if not isinstance(body, dict):
        raise RuntimeError(f"GET /data/identity returned unexpected body: {type(body).__name__}")
    return body


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List LeanDNA metrics where ownerId matches the current API user (GET /data/identity).",
    )
    ap.add_argument(
        "--format",
        choices=("json", "brief"),
        default="json",
        help="json: array of metric objects (default); brief: tab-separated summary lines",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        metavar="ID",
        help="Value for RequestedSites header on identity + Metric GETs",
    )
    ap.add_argument(
        "--user-id",
        default=None,
        metavar="ID",
        help="Skip identity GET; filter catalog by this ownerId (debug / override)",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable INFO logging from BPO clients",
    )
    ap.add_argument(
        "--connect-timeout",
        type=float,
        default=15.0,
        metavar="SEC",
        help="TCP/TLS connect timeout (default: 15)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        dest="read_timeout",
        metavar="SEC",
        help="Read timeout after connect (default: 120)",
    )
    ns = ap.parse_args()

    bpo_log = logging.getLogger("bpo")
    bpo_log.setLevel(logging.INFO if ns.verbose else logging.WARNING)
    bpo_log.propagate = False

    if not leandna_data_api_credentials_configured():
        print(
            "Missing LeanDNA Data API credentials — set LEANDNA_DATA_API_BEARER_TOKEN and/or "
            "LEANDNA_DATA_API_COOKIE in .env.",
            file=sys.stderr,
        )
        return 1

    try:
        base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    bucket = BPO_LEANDNA_DATA_API_EXECUTION_BUCKET
    print(
        f"LeanDNA target: GET {base}/data/identity + GET {base}/data/Metric  "
        f"(EXECUTION_ENV bucket: {bucket})",
        file=sys.stderr,
    )

    owner_id = (str(ns.user_id).strip() if ns.user_id is not None else "") or None
    owner_label = ""

    if owner_id is None:
        try:
            identity = _fetch_identity(
                requested_sites=ns.requested_sites,
                read_timeout_seconds=ns.read_timeout,
            )
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            return 1
        except requests.Timeout as e:
            print(f"LeanDNA identity request timed out: {e}", file=sys.stderr)
            return 1
        owner_id = _identity_user_id(identity)
        if not owner_id:
            print("GET /data/identity did not return userId.", file=sys.stderr)
            return 1
        owner_label = _identity_display_name(identity)
        print(
            f"Session user: {owner_label or '(no userName)'} (userId={owner_id})",
            file=sys.stderr,
        )
    else:
        print(f"Using --user-id {owner_id!r} (identity GET skipped).", file=sys.stderr)

    try:
        catalog = list_metric_definitions(
            requested_sites=ns.requested_sites,
            connect_timeout_seconds=ns.connect_timeout,
            timeout_seconds=ns.read_timeout,
            extra_query=None,
        )
    except requests.Timeout as e:
        print(f"LeanDNA Metric catalog request timed out: {e}", file=sys.stderr)
        return 1
    except requests.HTTPError as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        code = getattr(e.response, "status_code", None) if e.response is not None else None
        if code == 401:
            print(
                "LeanDNA returned 401 — align base URL and credentials with EXECUTION_ENV (ST_/PR_*).",
                file=sys.stderr,
            )
        return 1
    except Exception as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    catalog_count = len(catalog)
    rows = _sort_rows(
        [
            r
            for r in catalog
            if isinstance(r, dict) and _id_matches(r.get("ownerId"), owner_id)
        ]
    )

    if ns.format == "brief":
        print("id\tname\tmetricType\tsiteId\townerId\tcategories\tvalueStreams")
        for ln in _brief_lines(rows):
            print(ln)
    else:
        print(json.dumps(rows, indent=2, default=str, ensure_ascii=False))

    n = len(rows)
    print(
        f"Displayed {n} metric{'s' if n != 1 else ''} owned by userId={owner_id!r} "
        f"(of {catalog_count} in catalog).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
