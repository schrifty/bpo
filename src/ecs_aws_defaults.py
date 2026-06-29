"""Shared AWS ECS / EventBridge defaults for CLI tooling."""

from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TFVARS_PATH = _PROJECT_ROOT / "infra" / "terraform" / "terraform.tfvars"


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _is_aws_runtime() -> bool:
    return bool(
        os.environ.get("AWS_EXECUTION_ENV")
        or os.environ.get("ECS_CONTAINER_METADATA_URI")
        or os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    )


def _should_skip_dotenv() -> bool:
    if _truthy_env("CORTEX_SKIP_DOTENV"):
        return True
    if os.environ.get("CORTEX_SKIP_DOTENV", "").strip().lower() in ("0", "false", "no", "off"):
        return False
    return _is_aws_runtime()


def _migrate_legacy_bpo_env() -> None:
    for key, val in list(os.environ.items()):
        if key.startswith("BPO_"):
            cortex_key = "CORTEX_" + key[4:]
            if not os.environ.get(cortex_key, "").strip():
                os.environ[cortex_key] = val


def _ensure_local_env_loaded() -> None:
    if not _should_skip_dotenv():
        load_dotenv(_PROJECT_ROOT / ".env")
    _migrate_legacy_bpo_env()


def _read_terraform_tfvars() -> dict[str, str]:
    if not _TFVARS_PATH.is_file():
        return {}
    out: dict[str, str] = {}
    for raw_line in _TFVARS_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value:
            out[key] = value
    return out


def _terraform_name_prefix() -> str | None:
    value = _read_terraform_tfvars().get("name_prefix", "").strip()
    return value or None


def _terraform_aws_region() -> str | None:
    value = _read_terraform_tfvars().get("aws_region", "").strip()
    return value or None


def default_region() -> str:
    _ensure_local_env_loaded()
    return (
        os.environ.get("CORTEX_AWS_REGION", "").strip()
        or os.environ.get("AWS_DEFAULT_REGION", "").strip()
        or os.environ.get("AWS_REGION", "").strip()
        or _terraform_aws_region()
        or "us-east-1"
    )


def default_name_prefix() -> str:
    _ensure_local_env_loaded()
    return (
        os.environ.get("CORTEX_SCHEDULE_NAME_PREFIX", "").strip()
        or os.environ.get("CORTEX_NAME_PREFIX", "").strip()
        or _terraform_name_prefix()
        or "cortex"
    )


def default_cluster_name() -> str:
    _ensure_local_env_loaded()
    return os.environ.get("CORTEX_ECS_CLUSTER", "").strip() or default_name_prefix()


def default_task_family() -> str:
    _ensure_local_env_loaded()
    explicit = os.environ.get("CORTEX_ECS_TASK_FAMILY", "").strip()
    if explicit:
        return explicit
    return f"{default_name_prefix()}-decks"


def format_cluster_not_found_error(*, cluster: str, region: str) -> str:
    prefix = default_name_prefix()
    family = default_task_family()
    tfvars_note = (
        f" (from { _TFVARS_PATH.relative_to(_PROJECT_ROOT) })"
        if _terraform_name_prefix()
        else ""
    )
    return (
        f"ECS cluster {cluster!r} was not found in {region}. "
        f"Resolved defaults: cluster={prefix!r}, family={family!r}{tfvars_note}. "
        f"Try: cortex --running --cluster {prefix} --family {family} --region {region}, "
        "or set CORTEX_ECS_CLUSTER / CORTEX_SCHEDULE_NAME_PREFIX in .env."
    )


def is_cluster_not_found_error(message: str) -> bool:
    return bool(re.search(r"ClusterNotFoundException|Cluster not found", message, re.I))
