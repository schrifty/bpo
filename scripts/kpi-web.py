#!/usr/bin/env python3
"""Run the Cortex KPI web view (read-only browser UI + API)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Cortex KPI web view")
    parser.add_argument("--host", default=None, help="Bind host (default CORTEX_KPI_WEB_HOST)")
    parser.add_argument("--port", type=int, default=None, help="Bind port (default 8080)")
    parser.add_argument(
        "--base-url",
        default=None,
        help="Public base URL for OAuth redirects (CORTEX_KPI_WEB_BASE_URL)",
    )
    parser.add_argument(
        "--dev-user",
        default=None,
        help="Enable local/dev auth as this email (sets ALLOW_DEV_AUTH + DEV_USER)",
    )
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="Do not pull KPI store from S3 (local SQLite only)",
    )
    parser.add_argument("--registry", default=None, help="Alternate my-metrics.yaml path")
    parser.add_argument("--owners", default=None, help="Alternate kpi_owners.yaml path")
    args = parser.parse_args(argv)

    import os

    if args.dev_user:
        os.environ["CORTEX_KPI_WEB_ALLOW_DEV_AUTH"] = "true"
        os.environ["CORTEX_KPI_WEB_DEV_USER"] = args.dev_user.strip().lower()
    if args.host:
        os.environ["CORTEX_KPI_WEB_HOST"] = args.host
    if args.port is not None:
        os.environ["CORTEX_KPI_WEB_PORT"] = str(args.port)
    if args.base_url:
        os.environ["CORTEX_KPI_WEB_BASE_URL"] = args.base_url.rstrip("/")
    if args.skip_s3:
        os.environ["CORTEX_KPI_WEB_SKIP_S3"] = "true"
    if args.registry:
        os.environ["CORTEX_KPI_WEB_REGISTRY"] = args.registry
    if args.owners:
        os.environ["CORTEX_KPI_WEB_OWNERS"] = args.owners

    from src.kpi_web.app import run

    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
