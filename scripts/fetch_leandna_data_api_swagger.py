#!/usr/bin/env python3
"""Fetch LeanDNA Data API OpenAPI/Swagger spec using env credentials and summarize endpoints.

Loads ``.env`` from the repo root (same pattern as ``src/config.py``).

Environment (set in ``.env``):
  DATA_API_BEARER_TOKEN   — sent as ``Authorization: Bearer <token>`` when non-empty
  DATA_API_KEY            — sent as an API key header when non-empty (see below)
  DATA_API_BASE_URL       — optional; default ``https://app.leandna.com/api``
  DATA_API_SWAGGER_URL    — optional full URL to the spec; default ``{BASE_URL}/swagger.json``
  DATA_API_KEY_HEADER     — optional header name for ``DATA_API_KEY``; default ``X-API-Key``.
    Some deployments use ``Api-Key``, ``x-api-key``, or a vendor-specific name — override if 401 persists.

Usage (repo root):

  python scripts/fetch_leandna_data_api_swagger.py
  python scripts/fetch_leandna_data_api_swagger.py --save docs/leandna-data-api-swagger.json
  python scripts/fetch_leandna_data_api_swagger.py --key-header Api-Key
  python scripts/fetch_leandna_data_api_swagger.py --probe-auth
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_BASE = "https://app.leandna.com/api"
DEFAULT_SWAGGER_PATH = "/swagger.json"


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
    except ImportError:
        pass


def _build_headers(
    bearer: str,
    api_key: str,
    key_header: str,
) -> dict[str, str]:
    h: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "bpo-fetch-leandna-swagger/1.0",
    }
    if bearer.strip():
        h["Authorization"] = f"Bearer {bearer.strip()}"
    if api_key.strip() and key_header.strip():
        h[key_header.strip()] = api_key.strip()
    return h


def _fetch_url(url: str, headers: dict[str, str], timeout: float = 60.0) -> tuple[int, bytes]:
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read()
    except HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        return e.code, body


def _iter_operations(spec: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize OpenAPI 3.x and Swagger 2.0 paths into a list of ops."""
    paths = spec.get("paths") or {}
    out: list[dict[str, Any]] = []
    for path, item in paths.items():
        if not isinstance(item, dict):
            continue
        for method in ("get", "post", "put", "patch", "delete", "head", "options"):
            if method not in item:
                continue
            op = item[method]
            if not isinstance(op, dict):
                continue
            out.append(
                {
                    "method": method.upper(),
                    "path": path,
                    "operation_id": op.get("operationId") or "",
                    "summary": (op.get("summary") or "").strip(),
                    "tags": op.get("tags") or ["untagged"],
                }
            )
    return out


def _print_tool_ideas(ops: list[dict[str, Any]]) -> None:
    """Heuristic suggestions for BPO / agent tools (read-only vs write)."""
    by_tag: dict[str, list[dict[str, Any]]] = {}
    for o in ops:
        for t in o["tags"]:
            by_tag.setdefault(str(t), []).append(o)

    print("\n" + "=" * 72)
    print("TOOL IDEAS (heuristic — refine after you read real summaries)")
    print("=" * 72)

    read_methods = frozenset({"GET", "HEAD"})
    writes = [o for o in ops if o["method"] not in read_methods]
    reads = [o for o in ops if o["method"] in read_methods]

    print(
        f"\n• Read-oriented operations: {len(reads)} — good candidates for reporting, "
        "CS metrics enrichment, or agent \"lookup\" tools (cache + QA)."
    )
    print(
        f"• Write / mutating operations: {len(writes)} — wrap with confirmation, "
        "idempotency notes, and strict allowlists if exposed to an LLM."
    )

    print("\nBy tag (sketch what a named tool might do):")
    for tag in sorted(by_tag.keys(), key=str.lower):
        tag_ops = by_tag[tag]
        n_get = sum(1 for x in tag_ops if x["method"] == "GET")
        n_post = sum(1 for x in tag_ops if x["method"] == "POST")
        print(f"  — {tag}: {len(tag_ops)} ops ({n_get} GET, {n_post} POST, …) → ")
        print(
            "      e.g. `leandna_data_{slug}` wrappers: list/query endpoints for dashboards; "
            "POST endpoints for imports or actions only if product allows."
        )

    print("\nConcrete patterns to implement in BPO (typical for data APIs):")
    print("  1. `leandna_data_health` — GET /health or /status if present (connectivity probe).")
    print("  2. Entity list tools — GET collection endpoints → paginate, normalize to JSON for slides/CSR.")
    print("  3. Entity detail tools — GET /{resource}/{id} for drill-down in speaker notes.")
    print("  4. Time-bounded exports — endpoints with date/query params → align to QBR quarter window.")
    print("  5. Optional write tools — only if spec shows safe, scoped POSTs; never expose raw POST to agent.")
    print("  6. Schema-driven field mapping — use swagger components/schemas to extend DATA_REGISTRY / CSR-style docs.")

    if not ops:
        print("\n(No operations parsed — check spec format.)")


def main() -> None:
    _load_dotenv()

    ap = argparse.ArgumentParser(description="Fetch LeanDNA Data API swagger.json with env auth.")
    ap.add_argument(
        "--base-url",
        default=os.environ.get("DATA_API_BASE_URL", DEFAULT_BASE).rstrip("/"),
        help=f"API base (default env DATA_API_BASE_URL or {DEFAULT_BASE})",
    )
    ap.add_argument(
        "--swagger-url",
        default=os.environ.get("DATA_API_SWAGGER_URL", "").strip(),
        help="Full swagger URL (default: {base}/swagger.json)",
    )
    ap.add_argument(
        "--key-header",
        default=os.environ.get("DATA_API_KEY_HEADER", "X-API-Key").strip() or "X-API-Key",
        help="Header name for DATA_API_KEY (default X-API-Key or DATA_API_KEY_HEADER)",
    )
    ap.add_argument("--save", metavar="FILE", help="Write raw JSON spec to this path")
    ap.add_argument(
        "--probe-auth",
        action="store_true",
        help="Try several common API-key header names and report HTTP status (no body print).",
    )
    ap.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout seconds")

    args = ap.parse_args()

    bearer = os.environ.get("DATA_API_BEARER_TOKEN", "") or ""
    api_key = os.environ.get("DATA_API_KEY", "") or ""

    swagger_url = args.swagger_url
    if not swagger_url:
        swagger_url = f"{args.base_url}{DEFAULT_SWAGGER_PATH}"

    if args.probe_auth and api_key:
        print("Probe: Bearer + API key under different header names (first success wins for status 200):")
        for hdr in [
            args.key_header,
            "X-API-Key",
            "Api-Key",
            "x-api-key",
            "X-Data-Api-Key",
            "Authorization",
        ]:
            if hdr == "Authorization" and bearer:
                continue
            h = _build_headers(bearer, api_key, hdr)
            code, _ = _fetch_url(swagger_url, h, timeout=args.timeout)
            print(f"  {hdr!r}: HTTP {code}")
        print()
    elif args.probe_auth:
        print("--probe-auth needs DATA_API_KEY in environment.", file=sys.stderr)
        sys.exit(2)

    if not bearer.strip() and not api_key.strip():
        print(
            "Set DATA_API_BEARER_TOKEN and/or DATA_API_KEY in .env (swagger may require one or both).",
            file=sys.stderr,
        )
        sys.exit(2)

    headers = _build_headers(bearer, api_key, args.key_header)
    print(f"GET {swagger_url}")
    code, body = _fetch_url(swagger_url, headers, timeout=args.timeout)

    if code != 200:
        print(f"HTTP {code}", file=sys.stderr)
        if body:
            try:
                print(body.decode("utf-8", errors="replace")[:2000], file=sys.stderr)
            except Exception:
                print(repr(body[:500]), file=sys.stderr)
        print(
            "\nHints: try --probe-auth with DATA_API_KEY set; set DATA_API_KEY_HEADER to the header "
            "your tenant expects; confirm DATA_API_BASE_URL matches the host that issued the token.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        spec = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if args.save:
        out = Path(args.save)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(body)
        print(f"Wrote {out} ({len(body)} bytes)")

    title = spec.get("info", {}).get("title", "") if isinstance(spec.get("info"), dict) else ""
    ver = spec.get("info", {}).get("version", "") if isinstance(spec.get("info"), dict) else ""
    openapi_ver = spec.get("openapi") or spec.get("swagger") or "?"

    print(f"\nSpec: {title!r} version {ver!r} ({openapi_ver})")

    ops = _iter_operations(spec)
    print(f"Operations: {len(ops)}")

    # Compact listing
    for o in sorted(ops, key=lambda x: (x["path"], x["method"])):
        oid = o["operation_id"] or "—"
        sm = o["summary"] or "—"
        tags = ",".join(o["tags"])
        print(f"  {o['method']:6} {o['path']:<48} [{tags}] {oid}: {sm[:60]}")

    _print_tool_ideas(ops)


if __name__ == "__main__":
    main()
