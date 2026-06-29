#!/usr/bin/env python3
"""Build AWS Secrets Manager JSON from repo-root ``.env`` (local use only).

Inlines ``GOOGLE_APPLICATION_CREDENTIALS`` → ``GOOGLE_SERVICE_ACCOUNT_JSON`` and
``SF_PRIVATE_KEY_PATH`` → ``SF_PRIVATE_KEY``. Does not commit output — write to a
gitignored path and paste into Secrets Manager console or ``aws secretsmanager create-secret``.

Usage:
  python3 scripts/build_secrets_manager_json.py
  python3 scripts/build_secrets_manager_json.py -o output/cortex-secrets-manager.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def _parse_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        if key:
            out[key] = val
    return out


def build_secrets_payload(env_path: Path | None = None) -> dict[str, Any]:
    path = env_path or (ROOT / ".env")
    if not path.is_file():
        raise FileNotFoundError(f".env not found: {path}")
    payload: dict[str, Any] = dict(_parse_dotenv(path))

    gac = payload.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    if gac:
        sa_path = Path(gac).expanduser()
        if not sa_path.is_absolute():
            sa_path = (ROOT / sa_path).resolve()
        payload["GOOGLE_SERVICE_ACCOUNT_JSON"] = json.loads(sa_path.read_text(encoding="utf-8"))

    sf_path = payload.pop("SF_PRIVATE_KEY_PATH", None)
    if sf_path and "SF_PRIVATE_KEY" not in payload:
        key_file = Path(sf_path).expanduser()
        if not key_file.is_absolute():
            key_file = (ROOT / key_file).resolve()
        payload["SF_PRIVATE_KEY"] = key_file.read_text(encoding="utf-8")

    # ECS sets these via task definition — omit from secret blob.
    for drop in ("CORTEX_SKIP_DOTENV", "CORTEX_SECRETS_ARN", "CORTEX_CACHE_DIR", "CORTEX_LOG_FORMAT"):
        payload.pop(drop, None)

    return payload


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Secrets Manager JSON from .env")
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=ROOT / "output" / "cortex-secrets-manager.json",
        help="Output path (default: output/cortex-secrets-manager.json)",
    )
    ap.add_argument("--env", type=Path, default=ROOT / ".env", help="Source .env file")
    args = ap.parse_args()
    payload = build_secrets_payload(args.env)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(payload)} keys → {args.output}", file=sys.stderr)
    print("Do not commit this file.", file=sys.stderr)


if __name__ == "__main__":
    main()
