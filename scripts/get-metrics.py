#!/usr/bin/env python3
"""List LeanDNA metric definitions from ``GET /data/Metric`` with associated fields.

Uses the same auth as the rest of BPO: ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or
``LEANDNA_DATA_API_COOKIE`` in the repo ``.env`` (see ``src/leandna_data_api_http.py``).

Examples::

  python3 scripts/get-metrics.py
  python3 scripts/get-metrics.py --format brief
  python3 scripts/get-metrics.py --requested-sites 416
  python3 scripts/get-metrics.py 638
  python3 scripts/get-metrics.py 638 --format brief
  python3 scripts/get-metrics.py --metric-id 638
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import warnings
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Before any code imports urllib3 (via requests).
warnings.filterwarnings(
    "ignore",
    message=r".*urllib3 v2 only supports OpenSSL.*",
    category=Warning,
)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

import requests  # noqa: E402

from src.config import BPO_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.leandna_data_api_http import leandna_data_api_credentials_configured  # noqa: E402
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metrics_client import list_metric_definitions  # noqa: E402


def _pop_leading_numeric_metric_id(argv: list[str]) -> tuple[list[str], str | None]:
    """If ``argv[1]`` is all digits, drop it and return it as the metric id (shorthand for ``--metric-id``)."""
    if len(argv) < 2:
        return argv, None
    token = argv[1]
    if token.startswith("-"):
        return argv, None
    s = token.strip()
    if s and s.isdigit():
        return [argv[0]] + argv[2:], s
    return argv, None


def _id_matches(raw_id: Any, want_s: str) -> bool:
    """True when catalog row ``id`` equals ``want_s`` (numeric or string match)."""
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
    """One line per metric: stable columns + pipe-separated extra keys for scanning."""
    lines = []
    for r in rows:
        mid = r.get("id", "")
        name = str(r.get("name") or r.get("crossSiteName") or "").replace("\t", " ").replace("\n", " ")
        mtype = str(r.get("metricType") or "")
        site = str(r.get("siteId") or "")
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
            f"{mid}\t{name}\t{mtype}\tsiteId={site}\tcategories={cat_s}\tvalueStreams={vs_s}"
        )
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List LeanDNA metrics (GET /data/Metric).",
        epilog="If the first argument is all digits (e.g. get-metrics 638), it is treated as --metric-id.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--format",
        choices=("json", "brief"),
        default="json",
        help="json: full API objects (default); brief: tab-separated summary lines",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        metavar="ID",
        help="Value for RequestedSites header (often required for tenant-specific views)",
    )
    ap.add_argument(
        "--metric-id",
        default=None,
        metavar="ID",
        help="Return only this metric (same catalog GET; filtered client-side by id)",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable INFO logging from BPO clients (default: warnings only)",
    )
    ap.add_argument(
        "--connect-timeout",
        type=float,
        default=15.0,
        metavar="SEC",
        help="TCP/TLS connect timeout for LeanDNA GET (default: 15)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        dest="read_timeout",
        metavar="SEC",
        help="Read timeout after connect for LeanDNA GET (default: 120)",
    )
    argv_mod, leading_metric_id = _pop_leading_numeric_metric_id(list(sys.argv))
    ns = ap.parse_args(argv_mod[1:])

    flag_metric_id = str(ns.metric_id).strip() if ns.metric_id is not None else None
    if flag_metric_id == "":
        flag_metric_id = None
    if leading_metric_id is not None and flag_metric_id is not None and leading_metric_id != flag_metric_id:
        print(
            f"Conflicting metric id: leading argument {leading_metric_id!r} vs "
            f"--metric-id {flag_metric_id!r}.",
            file=sys.stderr,
        )
        return 1
    metric_id_filter = flag_metric_id or leading_metric_id

    # Avoid duplicate lines: ``bpo`` has its own StreamHandler (src.config) and also
    # propagates to root; ``basicConfig`` adds a root handler that prints the same record again.
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
        _base = data_api_base_url()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    print(
        f"LeanDNA target: GET {_base}/data/Metric  "
        f"(EXECUTION_ENV bucket: {BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
        file=sys.stderr,
    )

    try:
        rows = list_metric_definitions(
            requested_sites=ns.requested_sites,
            connect_timeout_seconds=ns.connect_timeout,
            timeout_seconds=ns.read_timeout,
            extra_query=None,
        )
    except requests.Timeout as e:
        print(f"LeanDNA request timed out: {e}", file=sys.stderr)
        print(
            "Try --connect-timeout / --timeout, or check VPN, DNS, and LEANDNA base URL (EXECUTION_ENV + ST_/PR_*).",
            file=sys.stderr,
        )
        return 1
    except requests.HTTPError as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        code = getattr(e.response, "status_code", None) if e.response is not None else None
        if code == 401:
            print(
                "LeanDNA returned 401 — token/session does not match this API host. "
                "Use LEANDNA_DATA_API_BASE_URL for the same environment as your cookie "
                "(e.g. staging base URL + Cookie from a staging browser session), or refresh "
                "LEANDNA_DATA_API_COOKIE / LEANDNA_DATA_API_BEARER_TOKEN.",
                file=sys.stderr,
            )
        return 1
    except Exception as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    rows = _sort_rows(rows)
    catalog_count = len(rows)

    if metric_id_filter is not None:
        want = metric_id_filter
        rows = [r for r in rows if _id_matches(r.get("id"), want)]
        if not rows:
            print(
                f"No metric with id={want!r} in GET /data/Metric response "
                f"({catalog_count} definition(s) in catalog).",
                file=sys.stderr,
            )
            return 1
    elif not rows:
        print("No metric definitions returned.", file=sys.stderr)
        return 1

    if ns.format == "brief":
        print("id\tname\tmetricType\tsiteId\tcategories\tvalueStreams")
        for ln in _brief_lines(rows):
            print(ln)
    else:
        single_object = metric_id_filter is not None and len(rows) == 1
        payload: Any = rows[0] if single_object else rows
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))

    n = len(rows)
    summary = f"Displayed {n} metric{'s' if n != 1 else ''}."
    print(summary, file=sys.stderr)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
