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

AUTOMATION_VALUES = ("automated", "manual", "blocked")
TARGET_DIRECTIONS = ("higher", "lower")

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
    sources = row.get("data_source")
    if isinstance(sources, str) and sources.strip():
        sources = [sources.strip()]
    if (
        not isinstance(sources, list)
        or not sources
        or any(not str(item).strip() for item in sources)
    ):
        raise HealthScoreFrameworkError(
            f"component {row.get('key')!r} data_source must be a non-empty list"
        )
    automation = str(row.get("automation") or "").strip().lower()
    if automation not in AUTOMATION_VALUES:
        raise HealthScoreFrameworkError(
            f"component {row.get('key')!r} automation must be one of "
            f"{list(AUTOMATION_VALUES)}, got {row.get('automation')!r}"
        )
    target = row.get("target")
    direction = row.get("target_direction")
    if target is not None:
        if isinstance(target, bool) or not isinstance(target, (int, float)):
            raise HealthScoreFrameworkError(
                f"component {row.get('key')!r} target must be a number or null, got {target!r}"
            )
        direction = str(direction or "").strip().lower()
        if direction not in TARGET_DIRECTIONS:
            raise HealthScoreFrameworkError(
                f"component {row.get('key')!r} target_direction must be one of "
                f"{list(TARGET_DIRECTIONS)} when a target is set, got {row.get('target_direction')!r}"
            )
    elif direction is not None:
        raise HealthScoreFrameworkError(
            f"component {row.get('key')!r} has a target_direction but no target"
        )
    return {
        **row,
        "grain": grain,
        "owner": str(row.get("owner") or default_owner).strip(),
        "metric-id": row.get("metric-id"),
        "metric-generator": row.get("metric-generator"),
        "data_source": [str(item).strip() for item in sources],
        "automation": automation,
        "target": target,
        "target_direction": direction if target is not None else None,
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
    payload["available_data_sources"] = available_data_source_names()
    payload["available_source_labels"] = available_source_labels()
    return payload


def _data_source_registry() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "data_source_registry.yaml"
    try:
        registry = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise HealthScoreFrameworkError(
            f"could not load data source registry {path}: {exc}"
        ) from exc
    if not isinstance(registry, dict):
        raise HealthScoreFrameworkError("data source registry must be a YAML object")
    return registry


def available_source_labels() -> list[str]:
    """Labels of business systems Cortex can read, in registry order.

    ``healthscore_label`` overrides ``display_name`` for the Healthscore UI only.
    """
    registry = _data_source_registry()
    sources = registry.get("sources") or {}
    if not isinstance(sources, dict):
        return []
    order = registry.get("source_display_order") or list(sources)
    labels: list[str] = []
    seen: set[str] = set()
    for key in [*order, *sources]:
        if key in seen:
            continue
        seen.add(key)
        meta = sources.get(key) or {}
        if not isinstance(meta, dict):
            continue
        label = str(meta.get("healthscore_label") or meta.get("display_name") or "").strip()
        if label:
            labels.append(label)
    return labels


def available_data_source_names() -> list[str]:
    """Display names and aliases of business systems Cortex can read.

    Healthscore source chips treat every other label as not yet available.
    """
    registry = _data_source_registry()
    names: list[str] = []
    seen: set[str] = set()
    for meta in (registry.get("sources") or {}).values():
        if not isinstance(meta, dict):
            continue
        aliases = meta.get("aliases") or []
        if isinstance(aliases, str):
            aliases = [aliases]
        for raw in (meta.get("display_name"), meta.get("healthscore_label"), *aliases):
            label = str(raw or "").strip()
            key = label.casefold()
            if not label or key in seen:
                continue
            seen.add(key)
            names.append(label)
    return names


_UNSET: Any = object()

# Fields a catalog admin may change from the UI. Everything else in the
# framework stays a hand-edited YAML decision.
EDITABLE_INPUT_FIELDS = ("name", "pillar", "weight", "description", "owner", "grain", "deactivated")
EDITABLE_OVERRIDE_FIELDS = ("name", "description", "owner", "grain", "deactivated")


def _split_header(text: str) -> str:
    """Leading comment/blank lines, kept verbatim when the file is rewritten."""
    header: list[str] = []
    for line in text.splitlines(keepends=True):
        if line.strip() == "" or line.lstrip().startswith("#"):
            header.append(line)
            continue
        break
    return "".join(header)


def _clean_weight(raw: Any) -> float | int | None:
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise HealthScoreFrameworkError(f"weight must be a number or null, got {raw!r}") from exc
    if value < 0:
        raise HealthScoreFrameworkError("weight cannot be negative")
    return int(value) if value.is_integer() else value


def _clean_grain(raw: Any) -> str | None:
    clean = str(raw or "").strip().lower()
    if not clean:
        return None
    if clean not in GRAINS:
        raise HealthScoreFrameworkError(
            f"grain must be one of {sorted(GRAINS)} or null, got {raw!r}"
        )
    return clean


def update_component(
    key: str,
    *,
    name: Any = _UNSET,
    pillar: Any = _UNSET,
    weight: Any = _UNSET,
    description: Any = _UNSET,
    owner: Any = _UNSET,
    grain: Any = _UNSET,
    deactivated: Any = _UNSET,
    path: Path | None = None,
) -> dict[str, Any]:
    """Rewrite one component's editable fields in the framework YAML.

    Inputs accept ``name``, ``pillar``, ``weight``, ``description``, ``owner``
    and ``grain``; override flags accept everything except ``pillar`` and
    ``weight``. ``deactivated`` pauses either kind of component without
    changing ``automation`` or ``status``. Deactivating an input parks its
    weight in ``deactivated_weight`` and sets ``weight`` to 0 so the component
    drops out of the configured total; reactivating puts the parked weight
    back. The weight cannot be edited while the input is deactivated.

    ``configured_weight`` is recomputed from the input weights so the file keeps
    validating. Returns the reloaded framework. Raises
    :class:`HealthScoreFrameworkError` and leaves the file untouched on any
    invalid edit.
    """
    source = path or framework_path()
    try:
        original = source.read_text(encoding="utf-8")
        payload = yaml.safe_load(original)
    except (OSError, yaml.YAMLError) as exc:
        raise HealthScoreFrameworkError(
            f"could not load Health Score framework {source}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise HealthScoreFrameworkError("Health Score framework must be a YAML object")
    inputs = payload.get("inputs") or []
    overrides = payload.get("overrides") or []
    target: dict[str, Any] | None = None
    is_input = False
    for row in inputs:
        if isinstance(row, dict) and str(row.get("key")) == key:
            target, is_input = row, True
            break
    if target is None:
        for row in overrides:
            if isinstance(row, dict) and str(row.get("key")) == key:
                target = row
                break
    if target is None:
        raise HealthScoreFrameworkError(f"unknown Health Score component: {key}")

    changed: dict[str, Any] = {}
    if name is not _UNSET:
        clean = str(name or "").strip()
        if not clean:
            raise HealthScoreFrameworkError("name cannot be empty")
        changed["name"] = clean
    if pillar is not _UNSET:
        if not is_input:
            raise HealthScoreFrameworkError("override flags have no pillar")
        clean = str(pillar or "").strip()
        if not clean:
            raise HealthScoreFrameworkError("pillar cannot be empty")
        changed["pillar"] = clean
    already_deactivated = bool(target.get("deactivated"))
    if weight is not _UNSET:
        if not is_input:
            raise HealthScoreFrameworkError("override flags carry no weight")
        if already_deactivated or (deactivated is not _UNSET and deactivated):
            raise HealthScoreFrameworkError(
                "weight is held at 0 while the input is deactivated; reactivate it first"
            )
        changed["weight"] = _clean_weight(weight)
    if description is not _UNSET:
        clean = str(description or "").strip()
        if not clean:
            raise HealthScoreFrameworkError("description cannot be empty")
        changed["description"] = clean
    if owner is not _UNSET:
        clean = str(owner or "").strip()
        if not clean:
            raise HealthScoreFrameworkError("owner cannot be empty")
        changed["owner"] = clean
    if grain is not _UNSET:
        changed["grain"] = _clean_grain(grain)
    deactivated_touched = deactivated is not _UNSET
    if deactivated_touched:
        if deactivated:
            changed["deactivated"] = True
            if is_input and not already_deactivated:
                # Park the weight so the input stops counting toward the total
                # and the original value comes back on reactivation.
                target["deactivated_weight"] = target.get("weight")
                changed["weight"] = 0
        else:
            target.pop("deactivated", None)
            if is_input and "deactivated_weight" in target:
                changed["weight"] = target.pop("deactivated_weight")
    if not changed and not deactivated_touched:
        raise HealthScoreFrameworkError(
            "no editable field supplied (name, pillar, weight, description, owner, grain)"
        )
    target.update(changed)

    total = 0.0
    for row in inputs:
        if isinstance(row, dict) and row.get("weight") is not None:
            total += float(row["weight"])
    payload["configured_weight"] = int(total) if total.is_integer() else total

    body = yaml.safe_dump(
        payload, sort_keys=False, allow_unicode=True, width=100, default_flow_style=False
    )
    source.write_text(_split_header(original) + body, encoding="utf-8")
    try:
        return load_framework(source)
    except HealthScoreFrameworkError:
        source.write_text(original, encoding="utf-8")
        raise


def component_map(framework: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row["key"]): row
        for row in [*(framework.get("inputs") or []), *(framework.get("overrides") or [])]
    }
