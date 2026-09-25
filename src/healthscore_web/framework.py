"""Load and validate the draft Customer Health Score framework.

Component fields use the ``config/my-metrics.yaml`` names (``description``,
``owner``, ``metric-id``, ``metric-generator``, ``grain``, ``tags``) so the two
catalogs can merge later.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from src.kpi_store import GRAINS

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FRAMEWORK_PATH = PROJECT_ROOT / "config" / "healthscore_framework.yaml"


class HealthScoreFrameworkError(RuntimeError):
    pass


def framework_path() -> Path:
    raw = os.environ.get("CORTEX_HEALTHSCORE_FRAMEWORK", "").strip()
    return Path(raw).expanduser().resolve() if raw else DEFAULT_FRAMEWORK_PATH


def _normalize_component(row: dict[str, Any], default_owner: str) -> dict[str, Any]:
    grain = row.get("grain")
    if grain is not None:
        grain = str(grain).strip().lower()
        if grain not in GRAINS:
            raise HealthScoreFrameworkError(
                f"component {row.get('key')!r} grain must be one of "
                f"{sorted(GRAINS)} or null, got {row.get('grain')!r}"
            )
    tags = row.get("tags") or []
    if not isinstance(tags, list):
        raise HealthScoreFrameworkError(f"component {row.get('key')!r} tags must be a list")
    return {
        **row,
        "grain": grain,
        "owner": str(row.get("owner") or default_owner).strip(),
        "metric-id": row.get("metric-id"),
        "metric-generator": row.get("metric-generator"),
        "tags": [str(tag).strip().lower() for tag in tags],
    }


def load_framework(path: Path | None = None) -> dict[str, Any]:
    source = path or framework_path()
    try:
        payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise HealthScoreFrameworkError(
            f"could not load Health Score framework {source}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise HealthScoreFrameworkError("Health Score framework must be a YAML object")
    inputs = payload.get("inputs")
    overrides = payload.get("overrides")
    if not isinstance(inputs, list) or not isinstance(overrides, list):
        raise HealthScoreFrameworkError(
            "Health Score framework requires inputs and overrides lists"
        )
    default_owner = str(payload.get("owner") or "").strip()
    if not default_owner:
        raise HealthScoreFrameworkError("Health Score framework requires a default owner email")
    keys: set[str] = set()
    for row in [*inputs, *overrides]:
        if not isinstance(row, dict):
            raise HealthScoreFrameworkError("every Health Score component must be an object")
        key = str(row.get("key") or "").strip()
        if not key or key in keys:
            raise HealthScoreFrameworkError(f"invalid or duplicate component key: {key!r}")
        keys.add(key)
    payload["inputs"] = [_normalize_component(row, default_owner) for row in inputs]
    payload["overrides"] = [_normalize_component(row, default_owner) for row in overrides]
    actual_weight = sum(
        float(row["weight"])
        for row in payload["inputs"]
        if row.get("weight") is not None
    )
    expected_weight = float(payload.get("configured_weight") or 0)
    if abs(actual_weight - expected_weight) > 1e-9:
        raise HealthScoreFrameworkError(
            f"configured Health Score weight is {actual_weight:g}, expected {expected_weight:g}"
        )
    payload["configured_weight"] = actual_weight
    payload["input_count"] = len(payload["inputs"])
    payload["override_count"] = len(payload["overrides"])
    return payload


def component_map(framework: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row["key"]): row
        for row in [*(framework.get("inputs") or []), *(framework.get("overrides") or [])]
    }
