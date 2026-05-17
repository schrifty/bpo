#!/usr/bin/env python3
"""Copy a LeanDNA metric from production (PR_*) to staging (ST_*).

Reads the production metric definition by id, creates a **new** metric on staging (new id),
then copies production ``MetricDataPoint`` values into that staging metric.

Uses ``PR_*`` and ``ST_*`` from ``.env`` only — **``EXECUTION_ENV`` is ignored**.
Unrelated to ``get-help-ttr`` (Jira-only).

Requires in ``.env``:
  ``PR_LEANDNA_DATA_API_BASE_URL`` + ``PR_LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``PR_LEANDNA_DATA_API_COOKIE``
  ``ST_LEANDNA_DATA_API_BASE_URL`` + ``ST_LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``ST_LEANDNA_DATA_API_COOKIE``

Examples::

  copy-metric-prod-to-staging 1911
  copy-metric-prod-to-staging 1911 --requested-sites 416
  copy-metric-prod-to-staging 1911 --dry-run
  copy-metric-prod-to-staging 2171 --lookback-days 90
  copy-metric-prod-to-staging 2171 --staging-site-id 416
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.leandna_data_api_env import leandna_env_credentials_configured  # noqa: E402
from src.leandna_metrics_copy import copy_metric_production_to_staging  # noqa: E402


def _print_brief(result: dict[str, Any]) -> None:
    if not result.get("ok"):
        if result.get("error"):
            print(f"Error: {result['error']}", file=sys.stderr)
        prod = result.get("production") or {}
        if prod.get("metric_id") is not None or prod.get("name"):
            print(
                f"Production metric: {prod.get('name')} "
                f"(id={prod.get('metric_id')}, siteId={prod.get('siteId')})"
            )
            print(f"  {prod.get('base_url')}")
        return

    prod = result.get("production") or {}
    stg = result.get("staging") or {}
    print(f"Production metric: {prod.get('name')} (id={prod.get('metric_id')}, siteId={prod.get('siteId')})")
    print(f"  {prod.get('base_url')}")
    if result.get("dry_run"):
        print("DRY RUN — no writes")
    if stg.get("metric_id") is not None:
        label = "Staging metric id" if not result.get("dry_run") else "Staging target id (existing or would create)"
        print(f"{label}: {stg.get('metric_id')}")
        if stg.get("reused_existing_by_name"):
            print("  (reused existing staging metric with same name — POST /data/Metric did not return id)")
    print(f"  {stg.get('base_url')}")
    dp = result.get("datapoints") or {}
    if dp.get("skipped"):
        print("Datapoints: not copied (--no-datapoints)")
    elif result.get("dry_run"):
        print(f"Datapoints: would copy {dp.get('source_count', 0)} row(s)")
    else:
        print(
            f"Datapoints: {dp.get('posted', 0)} posted, {dp.get('failed', 0)} failed "
            f"(from {dp.get('source_count', 0)} production points)"
        )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Copy a LeanDNA metric definition and MetricDataPoint values from production (PR_*) "
            "to a new metric on staging (ST_*)."
        ),
    )
    ap.add_argument(
        "metric_id",
        help="Production catalog metric id to copy",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        metavar="N",
        help="MetricDataPoint window when dates omitted (default: 365)",
    )
    ap.add_argument("--start-date", default=None, metavar="YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, metavar="YYYY-MM-DD")
    ap.add_argument(
        "--staging-site-id",
        default=None,
        metavar="ID",
        help="Override siteId on the staging metric definition",
    )
    ap.add_argument(
        "--requested-sites",
        default=None,
        metavar="ID",
        help="RequestedSites for production reads only (catalog + datapoints); defaults to metric siteId",
    )
    ap.add_argument(
        "--staging-requested-sites",
        default=None,
        metavar="ID",
        help="RequestedSites for staging only (omit unless your ST_* user has that site)",
    )
    ap.add_argument(
        "--no-datapoints",
        action="store_true",
        help="Copy definition only (skip MetricDataPoint POSTs)",
    )
    ap.add_argument(
        "--no-reuse-by-name",
        action="store_true",
        help="Do not fall back to an existing staging metric with the same name",
    )
    ap.add_argument("--dry-run", action="store_true", help="Show planned actions without writing")
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument("--timeout", type=float, default=120.0, dest="read_timeout", metavar="SEC")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    bpo_log = logging.getLogger("bpo")
    bpo_log.setLevel(logging.INFO if ns.verbose else logging.WARNING)
    bpo_log.propagate = False

    if not leandna_env_credentials_configured("production"):
        print(
            "Missing production LeanDNA credentials — set PR_LEANDNA_DATA_API_BASE_URL and "
            "PR_LEANDNA_DATA_API_BEARER_TOKEN and/or PR_LEANDNA_DATA_API_COOKIE.",
            file=sys.stderr,
        )
        return 1
    if not ns.dry_run and not leandna_env_credentials_configured("staging"):
        print(
            "Missing staging LeanDNA credentials — set ST_LEANDNA_DATA_API_BASE_URL and "
            "ST_LEANDNA_DATA_API_BEARER_TOKEN and/or ST_LEANDNA_DATA_API_COOKIE.",
            file=sys.stderr,
        )
        return 1

    staging_site = (str(ns.staging_site_id).strip() if ns.staging_site_id is not None else None) or None

    result = copy_metric_production_to_staging(
        ns.metric_id,
        lookback_days=ns.lookback_days,
        start_date=ns.start_date,
        end_date=ns.end_date,
        staging_site_id=staging_site,
        requested_sites=ns.requested_sites,
        staging_requested_sites=ns.staging_requested_sites,
        copy_datapoints=not ns.no_datapoints,
        reuse_staging_by_name=not ns.no_reuse_by_name,
        dry_run=ns.dry_run,
        timeout_seconds=ns.read_timeout,
    )

    if ns.format == "json":
        print(json.dumps(result, indent=2, default=str, ensure_ascii=False))
    else:
        _print_brief(result)

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
