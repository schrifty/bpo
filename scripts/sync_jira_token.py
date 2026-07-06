#!/usr/bin/env python3
"""Merge ``JIRA_API_TOKEN`` from ``.env`` into AWS Secrets Manager.

Updates only that key — other secret fields are preserved.

Typical workflow after refreshing the token locally::

  sync_jira_token
  sync_jira_token --dry-run

One-time global install (``~/.local/bin`` on PATH)::

  bin/sync_jira_token --install

Secret target resolution (first match):

  1. ``--secret-id`` / ``CORTEX_SECRETS_ARN``
  2. ``terraform -chdir=infra/terraform output -raw secrets_manager_arn``
  3. ``cortex/prod/env``
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

JIRA_TOKEN_KEY = "JIRA_API_TOKEN"


def parse_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        if key:
            out[key] = val
    return out


def token_fingerprint(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return "empty"
    digest = hashlib.sha256(text.encode()).hexdigest()[:12]
    return f"len={len(text)} sha256={digest}"


def merge_jira_token_from_dotenv(
    existing: dict[str, Any],
    dotenv: dict[str, str],
) -> tuple[dict[str, Any], list[str]]:
    """Return ``(merged_secret, updated_key_names)``."""
    merged = dict(existing)
    updated: list[str] = []
    if JIRA_TOKEN_KEY not in dotenv:
        return merged, updated
    new_val = dotenv[JIRA_TOKEN_KEY]
    if not str(new_val).strip():
        return merged, updated
    if merged.get(JIRA_TOKEN_KEY) != new_val:
        merged[JIRA_TOKEN_KEY] = new_val
        updated.append(JIRA_TOKEN_KEY)
    return merged, updated


def resolve_secret_id(explicit: str | None) -> str:
    if explicit and explicit.strip():
        return explicit.strip()
    import os

    arn = (os.environ.get("CORTEX_SECRETS_ARN") or "").strip()
    if arn:
        return arn
    tf_dir = ROOT / "infra" / "terraform"
    try:
        proc = subprocess.run(
            ["terraform", f"-chdir={tf_dir}", "output", "-raw", "secrets_manager_arn"],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    except OSError:
        pass
    return "cortex/prod/env"


def _secretsmanager_client(region: str):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required; pip install boto3 (or use the project Docker image)"
        ) from exc
    return boto3.client("secretsmanager", region_name=region)


def fetch_secret_json(*, secret_id: str, region: str) -> dict[str, Any]:
    client = _secretsmanager_client(region)
    resp = client.get_secret_value(SecretId=secret_id)
    raw = resp.get("SecretString")
    if not raw:
        raise RuntimeError(f"Secret {secret_id!r} has no SecretString payload")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Secret {secret_id!r} JSON must be an object")
    return payload


def put_secret_json(*, secret_id: str, region: str, payload: dict[str, Any]) -> None:
    client = _secretsmanager_client(region)
    client.put_secret_value(
        SecretId=secret_id,
        SecretString=json.dumps(payload, ensure_ascii=False),
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sync JIRA_API_TOKEN from .env into AWS Secrets Manager.",
    )
    ap.add_argument("--env", type=Path, default=ROOT / ".env", help="Source .env (default: repo root)")
    ap.add_argument(
        "--secret-id",
        default=None,
        help="Secrets Manager id or ARN (default: CORTEX_SECRETS_ARN, terraform output, cortex/prod/env)",
    )
    ap.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing to Secrets Manager",
    )
    args = ap.parse_args()

    if not args.env.is_file():
        print(f".env not found: {args.env}", file=sys.stderr)
        return 1

    dotenv = parse_dotenv(args.env)
    token = (dotenv.get(JIRA_TOKEN_KEY) or "").strip()
    if not token:
        print(f"No {JIRA_TOKEN_KEY} found in {args.env}", file=sys.stderr)
        return 1

    secret_id = resolve_secret_id(args.secret_id)
    try:
        existing = fetch_secret_json(secret_id=secret_id, region=args.region)
    except Exception as exc:
        print(f"Failed to read secret {secret_id!r}: {exc}", file=sys.stderr)
        return 1

    merged, updated = merge_jira_token_from_dotenv(existing, dotenv)
    if not updated:
        print(f"No changes — secret {secret_id!r} already matches .env for {JIRA_TOKEN_KEY}")
        print(f"  {JIRA_TOKEN_KEY}: {token_fingerprint(token)}")
        return 0

    print(f"Secret: {secret_id}  region: {args.region}")
    print(
        f"  update {JIRA_TOKEN_KEY}: "
        f"{token_fingerprint(str(existing.get(JIRA_TOKEN_KEY) or ''))} → {token_fingerprint(token)}"
    )

    if args.dry_run:
        print("Dry run — secret not modified.")
        return 0

    try:
        put_secret_json(secret_id=secret_id, region=args.region, payload=merged)
    except Exception as exc:
        print(f"Failed to write secret {secret_id!r}: {exc}", file=sys.stderr)
        return 1

    print(f"Updated {len(updated)} key(s) in Secrets Manager.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
