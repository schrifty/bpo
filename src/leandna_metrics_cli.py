"""Argparse helpers and output formatting for LeanDNA metric CLIs."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from src.config import CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET
from src.leandna_metrics_write import (
    READ_TIMEOUT_S,
    MetricDeleteArgs,
    MetricWriteArgs,
)


def pop_leading_numeric_metric_id(argv: list[str]) -> tuple[list[str], str | None]:
    """If ``argv[1]`` is all digits, drop it and return it as a metric id shorthand."""
    if len(argv) < 2:
        return argv, None
    token = argv[1]
    if token.startswith("-"):
        return argv, None
    s = token.strip()
    if s and s.isdigit():
        return [argv[0]] + argv[2:], s
    return argv, None


def configure_cortex_logging(*, verbose: bool) -> None:
    cortex_log = logging.getLogger("cortex")
    cortex_log.setLevel(logging.INFO if verbose else logging.WARNING)
    cortex_log.propagate = False


def add_metric_write_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--metric-ndx",
        type=int,
        required=True,
        help="Metric catalog id (Data API id)",
    )
    ap.add_argument("--date", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--numerator", type=float, required=True)
    ap.add_argument("--denominator", type=float, default=1.0)
    ap.add_argument(
        "--category",
        default=None,
        help="MetricDataPoint category (default: from catalog when found, else empty)",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        help="RequestedSites header (default: metric siteId from catalog, else omitted)",
    )
    ap.add_argument(
        "--skip-catalog",
        action="store_true",
        help="Do not call GET /data/Metric to resolve siteId (use --requested-sites or omit header)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=READ_TIMEOUT_S,
        metavar="SEC",
        help=f"HTTP read timeout (default: {READ_TIMEOUT_S:.0f})",
    )
    ap.add_argument("-v", "--verbose", action="store_true")


def add_metric_delete_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--metric-ndx", type=int, required=True, help="Metric catalog id")
    ap.add_argument("--date", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--requested-sites", default=None)
    ap.add_argument("--skip-catalog", action="store_true")
    ap.add_argument(
        "--timeout",
        type=float,
        default=READ_TIMEOUT_S,
        metavar="SEC",
        help=f"HTTP read timeout (default: {READ_TIMEOUT_S:.0f})",
    )
    ap.add_argument("-v", "--verbose", action="store_true")


def metric_write_args_from_namespace(ns: argparse.Namespace) -> MetricWriteArgs:
    return MetricWriteArgs(
        metric_id=ns.metric_ndx,
        entry_date=ns.date,
        numerator=ns.numerator,
        denominator=ns.denominator,
        requested_sites=ns.requested_sites,
        category=ns.category,
        skip_catalog=ns.skip_catalog,
        timeout_seconds=ns.timeout,
        verbose=ns.verbose,
    )


def metric_delete_args_from_namespace(ns: argparse.Namespace) -> MetricDeleteArgs:
    return MetricDeleteArgs(
        metric_id=ns.metric_ndx,
        entry_date=ns.date,
        requested_sites=ns.requested_sites,
        skip_catalog=ns.skip_catalog,
        timeout_seconds=ns.timeout,
        verbose=ns.verbose,
    )


def print_result_env(env: dict[str, Any]) -> None:
    print(json.dumps(env, indent=2, default=str))
    if env.get("ok"):
        return
    hint = env.get("hint")
    if hint:
        print(hint, file=sys.stderr)
    insert = env.get("insert") if env.get("upsert") else env
    if isinstance(insert, dict):
        hint = insert.get("hint")
        if hint:
            print(hint, file=sys.stderr)
    err = str((insert or env).get("error") or "")
    if "mutations" in err.lower() and CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET == "production":
        print(
            "Production writes blocked — prefix command with CORTEX_ALLOW_PRODUCTION_MUTATIONS=true",
            file=sys.stderr,
        )
    elif (insert or env).get("status") == 401 or "session not found" in err.lower():
        print(
            "Bearer token expired or invalid — refresh PR_LEANDNA_DATA_API_BEARER_TOKEN "
            "from DevTools (Authorization header on any /api/data/… request while logged in).",
            file=sys.stderr,
        )
    elif (insert or env).get("status") == 504 or "504" in err:
        print(
            "LeanDNA returned 504 Gateway Timeout — retry in a minute; their API may be slow.",
            file=sys.stderr,
        )
