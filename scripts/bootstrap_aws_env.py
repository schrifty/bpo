#!/usr/bin/env python3
"""Load Cortex secrets from AWS Secrets Manager into the process environment.

Run before ``decks.py`` on ECS/Fargate (see ``scripts/run_job.sh``). When
``CORTEX_SECRETS_ARN`` is unset, exits 0 without changes so local runs keep using ``.env``.

Expected secret JSON keys mirror ``.env.example`` variable names. Optional
``GOOGLE_SERVICE_ACCOUNT_JSON`` (object or string) is written to a temp file and
``GOOGLE_APPLICATION_CREDENTIALS`` is set to that path.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any


def _load_secret_string(arn: str) -> str:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required to load CORTEX_SECRETS_ARN; pip install boto3 or unset CORTEX_SECRETS_ARN"
        ) from exc
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=arn)
    raw = resp.get("SecretString")
    if not raw:
        raise RuntimeError(f"Secret {arn!r} has no SecretString payload")
    return raw


def apply_secret_payload(payload: dict[str, Any]) -> None:
    sa_raw = payload.pop("GOOGLE_SERVICE_ACCOUNT_JSON", None)
    for key, val in payload.items():
        if val is None:
            continue
        if isinstance(val, (dict, list)):
            os.environ[key] = json.dumps(val)
        else:
            os.environ[str(key)] = str(val)
    if sa_raw is not None:
        if isinstance(sa_raw, str):
            sa_text = sa_raw
        else:
            sa_text = json.dumps(sa_raw)
        fd, path = tempfile.mkstemp(prefix="cortex-google-sa-", suffix=".json")
        os.close(fd)
        Path(path).write_text(sa_text, encoding="utf-8")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path


def bootstrap(*, secrets_arn: str | None = None) -> bool:
    """Load secrets when configured. Returns True when secrets were applied."""
    arn = (secrets_arn or os.environ.get("CORTEX_SECRETS_ARN", "")).strip()
    if not arn:
        return False
    raw = _load_secret_string(arn)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Secret {arn!r} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Secret {arn!r} JSON must be an object")
    apply_secret_payload(dict(payload))
    os.environ.setdefault("CORTEX_SKIP_DOTENV", "1")
    return True


def main() -> None:
    try:
        applied = bootstrap()
    except Exception as exc:
        print(f"bootstrap_aws_env: {exc}", file=sys.stderr)
        sys.exit(1)
    if applied:
        print("bootstrap_aws_env: loaded secrets from CORTEX_SECRETS_ARN", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
