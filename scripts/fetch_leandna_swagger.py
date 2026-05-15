#!/usr/bin/env python3
"""Download LeanDNA OpenAPI spec (Swagger JSON).

``https://app.leandna.com/api/swagger.json`` returns 401 without credentials.
Uses ``LEANDNA_DATA_API_BEARER_TOKEN`` from the repo ``.env`` (same as Data API).

Examples::

  python scripts/fetch_leandna_swagger.py
  python scripts/fetch_leandna_swagger.py -o docs/leandna-api-swagger.json
  python scripts/fetch_leandna_swagger.py --url https://app.leandna.com/api/swagger.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from src.config import LEANDNA_DATA_API_BEARER_TOKEN, logger, resolve_leandna_data_api_base_url  # noqa: E402


def _default_swagger_url() -> str:
    base = resolve_leandna_data_api_base_url()
    return f"{base}/swagger.json"


def main() -> int:
    ap = argparse.ArgumentParser(description="Download LeanDNA swagger.json (authenticated).")
    ap.add_argument(
        "--url",
        default=_default_swagger_url(),
        help="Swagger document URL (default: resolved Data API base + /swagger.json)",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=ROOT / "docs" / "leandna-api-swagger.json",
        help="Output path (default: docs/leandna-api-swagger.json)",
    )
    ns = ap.parse_args()

    token = (LEANDNA_DATA_API_BEARER_TOKEN or "").strip()
    if not token:
        print(
            "Missing LEANDNA_DATA_API_BEARER_TOKEN in .env — server returns 401 without it.",
            file=sys.stderr,
        )
        return 1

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        r = requests.get(ns.url, headers=headers, timeout=120)
    except requests.RequestException as e:
        print(f"Request failed: {e}", file=sys.stderr)
        return 1

    if r.status_code != 200:
        print(f"HTTP {r.status_code}: {r.text[:800]}", file=sys.stderr)
        return 1

    try:
        data = r.json()
    except json.JSONDecodeError:
        print("Response is not valid JSON.", file=sys.stderr)
        return 1

    out = ns.output.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    logger.info("Wrote %s (%d bytes)", out, out.stat().st_size)
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
