#!/usr/bin/env python3
"""Build AWS Secrets Manager JSON bundles from repo-root ``.env`` (local use only).

Splits credentials into ``google``, ``integrations``, ``llm``, and ``slack`` JSON
files so they can rotate independently. Inlines ``GOOGLE_APPLICATION_CREDENTIALS``
→ ``GOOGLE_SERVICE_ACCOUNT_JSON`` and ``SF_PRIVATE_KEY_PATH`` → ``SF_PRIVATE_KEY``.

Does not commit output — write to a gitignored path and upload with Terraform
(``secrets_json_dir``) or ``aws secretsmanager put-secret-value``.

Usage:
  python3 scripts/build_secrets_manager_json.py
  python3 scripts/build_secrets_manager_json.py -o output/cortex-secrets
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.secrets_bundles import SECRET_BUNDLES, split_secret_payload  # noqa: E402

DEFAULT_OUT_DIR = ROOT / "output" / "cortex-secrets"


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
    """Combined payload (all keys) after inlining file-based credentials."""
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

    return payload


def build_secret_bundles(env_path: Path | None = None) -> dict[str, dict[str, Any]]:
    return split_secret_payload(build_secrets_payload(env_path))


def main() -> None:
    ap = argparse.ArgumentParser(description="Build split Secrets Manager JSON from .env")
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Output directory (default: output/cortex-secrets). Writes google.json, "
        "integrations.json, llm.json, slack.json.",
    )
    ap.add_argument("--env", type=Path, default=ROOT / ".env", help="Source .env file")
    args = ap.parse_args()
    bundles = build_secret_bundles(args.env)
    out_dir = args.output
    if out_dir.suffix.lower() == ".json":
        # Old single-file path: write the directory next to it, plus a combined copy.
        combined_path = out_dir
        out_dir = out_dir.with_suffix("")
        combined: dict[str, Any] = {}
        for name in SECRET_BUNDLES:
            combined.update(bundles[name])
        combined_path.parent.mkdir(parents=True, exist_ok=True)
        combined_path.write_text(json.dumps(combined, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote combined {len(combined)} keys → {combined_path}", file=sys.stderr)
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in SECRET_BUNDLES:
        dest = out_dir / f"{name}.json"
        dest.write_text(json.dumps(bundles[name], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote {len(bundles[name])} keys → {dest}", file=sys.stderr)
    print("Do not commit these files.", file=sys.stderr)


if __name__ == "__main__":
    main()
