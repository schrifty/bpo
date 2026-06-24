#!/usr/bin/env python3
"""Load Cortex secrets from AWS Secrets Manager into the process environment.

Run before ``cortex.py`` on ECS/Fargate (see ``scripts/run_job.sh``). When
``CORTEX_SECRETS_ARN`` is unset, exits 0 without changes so local runs keep using ``.env``.

Expected secret JSON keys mirror ``.env.example`` variable names. Optional
``GOOGLE_SERVICE_ACCOUNT_JSON`` (object or string) is written to a temp file and
``GOOGLE_APPLICATION_CREDENTIALS`` is set to that path.

``run_job.sh`` uses ``--shell-export`` so variables are applied in the shell
before ``cortex.py`` starts (a plain subprocess would not propagate ``os.environ``).
"""

from __future__ import annotations

import json
import os
import shlex
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


def build_secret_env(payload: dict[str, Any], *, sa_dir: str | None = None) -> dict[str, str]:
    """Return env vars to set from a Secrets Manager JSON object."""
    data = dict(payload)
    sa_raw = data.pop("GOOGLE_SERVICE_ACCOUNT_JSON", None)
    env: dict[str, str] = {}
    for key, val in data.items():
        if val is None:
            continue
        if isinstance(val, (dict, list)):
            env[str(key)] = json.dumps(val)
        else:
            env[str(key)] = str(val)
    if sa_raw is not None:
        if isinstance(sa_raw, str):
            sa_text = sa_raw
        else:
            sa_text = json.dumps(sa_raw)
        if sa_dir:
            Path(sa_dir).mkdir(parents=True, exist_ok=True)
            sa_path = tempfile.NamedTemporaryFile(
                prefix="cortex-google-sa-",
                suffix=".json",
                dir=sa_dir,
                delete=False,
            )
            sa_path.close()
            path = sa_path.name
        else:
            fd, path = tempfile.mkstemp(prefix="cortex-google-sa-", suffix=".json")
            os.close(fd)
        Path(path).write_text(sa_text, encoding="utf-8")
        env["GOOGLE_APPLICATION_CREDENTIALS"] = path
    env.setdefault("CORTEX_SKIP_DOTENV", "1")
    return env


def apply_secret_payload(payload: dict[str, Any]) -> None:
    for key, val in build_secret_env(payload).items():
        os.environ[key] = val


def render_shell_exports(env: dict[str, str]) -> str:
    return "\n".join(f"export {key}={shlex.quote(val)}" for key, val in sorted(env.items()))


def load_secret_env(*, secrets_arn: str | None = None, sa_dir: str | None = None) -> dict[str, str]:
    arn = (secrets_arn or os.environ.get("CORTEX_SECRETS_ARN", "")).strip()
    if not arn:
        return {}
    raw = _load_secret_string(arn)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Secret {arn!r} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Secret {arn!r} JSON must be an object")
    return build_secret_env(payload, sa_dir=sa_dir)


def bootstrap(*, secrets_arn: str | None = None) -> bool:
    """Load secrets into the current process. Returns True when secrets were applied."""
    env = load_secret_env(secrets_arn=secrets_arn)
    if not env:
        return False
    for key, val in env.items():
        os.environ[key] = val
    return True


def main() -> None:
    shell_export = "--shell-export" in sys.argv
    try:
        if shell_export:
            env = load_secret_env(sa_dir="/tmp")
            if env:
                print(render_shell_exports(env))
            sys.exit(0)
        applied = bootstrap()
    except Exception as exc:
        print(f"bootstrap_aws_env: {exc}", file=sys.stderr)
        sys.exit(1)
    if applied:
        print("bootstrap_aws_env: loaded secrets from CORTEX_SECRETS_ARN", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
