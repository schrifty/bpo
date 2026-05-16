#!/usr/bin/env python3
"""Fetch LeanDNA metric definitions from ``GET /data/Metric`` by id or name substring.

If the argument is **all digits**, it selects the catalog row with that **id** (exactly one).

Otherwise the argument is a **case-insensitive substring** match on ``name`` and
``crossSiteName``; **all** matching metrics are returned (sorted by id).

Uses the same auth as ``get-metrics.py``: ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or
``LEANDNA_DATA_API_COOKIE`` in the repo ``.env``.

Examples::

  python3 scripts/get-metric.py 638
  python3 scripts/get-metric.py "job success"
  python3 scripts/get-metric.py sync --format brief
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
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metrics_client import list_metric_definitions  # noqa: E402


def _id_matches(raw_id: Any, want_s: str) -> bool:
    w = (want_s or "").strip()
    if not w:
        return False
    try:
        return int(raw_id) == int(w)
    except (TypeError, ValueError):
        return str(raw_id).strip() == w


def _is_catalog_id_token(s: str) -> bool:
    """True when ``s`` is non-empty and every character is a decimal digit (catalog id path)."""
    t = s.strip()
    return bool(t) and t.isdigit()


def _grep_name_substring(rows: list[Any], needle: str) -> list[dict]:
    """Rows whose ``name`` or ``crossSiteName`` contains ``needle`` (case-insensitive)."""
    n = (needle or "").strip().lower()
    if not n:
        return []
    out: list[dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        blob = f"{r.get('name', '')!s} {r.get('crossSiteName', '')!s}".lower()
        if n in blob:
            out.append(r)
    return out


def _sort_rows_by_id(rows: list[dict]) -> list[dict]:
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
        description="Fetch LeanDNA metric definition(s) (GET /data/Metric): by id or name substring.",
        epilog="All-digit argument = catalog id only. Any other text = substring grep on name fields.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "id_or_substring",
        metavar="ID_OR_SUBSTRING",
        help="All digits: catalog id (one metric). Otherwise: substring match on name / crossSiteName (all matches).",
    )
    ap.add_argument(
        "--format",
        choices=("json", "brief"),
        default="json",
        help="json: one object (id mode) or array (substring mode). brief: header + matching rows",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        metavar="ID",
        help="Value for RequestedSites header",
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
    ns = ap.parse_args()

    token = str(ns.id_or_substring).strip()
    if not token:
        print("ID or substring argument must not be empty.", file=sys.stderr)
        return 1

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
        catalog = list_metric_definitions(
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
                "Align LEANDNA_DATA_API_BASE_URL with your cookie environment, or refresh credentials.",
                file=sys.stderr,
            )
        return 1
    except Exception as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    n_cat = len(catalog)
    dict_rows = [r for r in catalog if isinstance(r, dict)]

    if _is_catalog_id_token(token):
        rows = [r for r in dict_rows if _id_matches(r.get("id"), token)]
        if not rows:
            print(
                f"No metric with id={token!r} in GET /data/Metric ({n_cat} definition(s)).",
                file=sys.stderr,
            )
            return 1
        if len(rows) > 1:
            print(f"Internal error: multiple catalog rows matched id={token!r}.", file=sys.stderr)
            return 1
    else:
        rows = _grep_name_substring(dict_rows, token)
        if not rows:
            print(
                f"No metrics matched substring {token!r} in name / crossSiteName "
                f"({n_cat} definition(s)).",
                file=sys.stderr,
            )
            return 1
        rows = _sort_rows_by_id(rows)

    n = len(rows)
    if ns.format == "brief":
        print("id\tname\tmetricType\tsiteId\tcategories\tvalueStreams")
        for ln in _brief_lines(rows):
            print(ln)
    else:
        if n == 1 and _is_catalog_id_token(token):
            payload: Any = rows[0]
        else:
            payload = rows
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))

    summary = f"Displayed {n} metric{'s' if n != 1 else ''}."
    print(summary, file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
