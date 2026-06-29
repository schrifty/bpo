"""Load and query ``config/my-metrics.yaml`` (LeanDNA metric registry)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .config_paths import METRICS_FILE

KPI_AUTOMATION_METRIC_NAME = "KPI Automation %"


def load_metrics_registry(*, path: Path | None = None) -> dict[str, Any]:
    """Parse ``config/my-metrics.yaml`` and return the document root."""
    metrics_path = path or METRICS_FILE
    if not metrics_path.is_file():
        raise FileNotFoundError(f"Metrics registry not found: {metrics_path}")
    data = yaml.safe_load(metrics_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping at root of {metrics_path}")
    return data


def has_metric_id(entry: Any) -> bool:
    """True when *entry* has a non-empty ``metric-id``."""
    if not isinstance(entry, dict):
        return False
    mid = entry.get("metric-id")
    return mid is not None and str(mid).strip() != ""


def registry_metric_description(entry: Any) -> str | None:
    """Optional human-readable ``description`` for a registry entry."""
    if not isinstance(entry, dict):
        return None
    raw = entry.get("description")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def registry_datapoint_metric_id(entry: Any) -> int | None:
    """Optional ``datapoint-metric-id`` when series data lives on another catalog row."""
    if not isinstance(entry, dict):
        return None
    raw = entry.get("datapoint-metric-id")
    if raw is None or str(raw).strip() == "":
        return None
    return int(raw)


def datapoint_metric_ids_for_entry(entry: dict[str, Any], metric_id: int) -> list[int]:
    """Catalog ids to query for latest values, registry id first then optional fallback."""
    ids = [metric_id]
    alt = registry_datapoint_metric_id(entry)
    if alt is not None and alt not in ids:
        ids.append(alt)
    return ids


def iter_metrics_with_id(
    *,
    registry: dict[str, Any] | None = None,
) -> list[tuple[str, int, dict[str, Any]]]:
    """Registry rows with ``metric-id`` set: ``(display name, id, entry dict)``."""
    reg = registry if registry is not None else load_metrics_registry()
    metrics = reg.get("metrics")
    if not isinstance(metrics, dict):
        return []
    out: list[tuple[str, int, dict[str, Any]]] = []
    for name, entry in metrics.items():
        if not isinstance(entry, dict):
            continue
        mid_raw = entry.get("metric-id")
        if mid_raw is None or str(mid_raw).strip() == "":
            continue
        out.append((str(name), int(mid_raw), entry))
    return out


def is_automated_metric(entry: Any) -> bool:
    """True when *entry* has a ``metric-generator`` (a generator means it is automated)."""
    return has_metric_generator(entry)


def count_automated_metrics(*, registry: dict[str, Any] | None = None) -> tuple[int, int]:
    """``(automated, total)`` metric counts across the registry."""
    reg = registry if registry is not None else load_metrics_registry()
    metrics = reg.get("metrics")
    if not isinstance(metrics, dict):
        return 0, 0
    entries = [entry for entry in metrics.values() if isinstance(entry, dict)]
    automated = sum(1 for entry in entries if is_automated_metric(entry))
    return automated, len(entries)


def has_metric_generator(entry: Any) -> bool:
    """True when *entry* has a non-empty ``metric-generator``."""
    if not isinstance(entry, dict):
        return False
    gen = entry.get("metric-generator")
    if gen is None:
        return False
    return not (isinstance(gen, str) and not gen.strip())


def is_fully_defined_metric(entry: Any) -> bool:
    """True when *entry* has ``metric-id`` and a non-empty ``metric-generator``."""
    if not isinstance(entry, dict):
        return False
    mid = entry.get("metric-id")
    if mid is None or str(mid).strip() == "":
        return False
    return has_metric_generator(entry)


def is_upsertable_metric(entry: Any) -> bool:
    """True when ``metrics-upsert`` can run the generator (requires ``metric-id`` or catalog lookup)."""
    return has_metric_generator(entry)


def metric_registry_skip_reason(entry: Any) -> str | None:
    """Why *entry* is omitted from ``metrics-upsert`` (``None`` when upsertable)."""
    if not isinstance(entry, dict):
        return "invalid registry row"
    if is_upsertable_metric(entry):
        return None
    if entry.get("metric-id") is not None and str(entry.get("metric-id")).strip() != "":
        return "no generator"
    return "no generator"


def count_fully_defined_metrics(*, registry: dict[str, Any] | None = None) -> int:
    """Count registry rows with both ``metric-id`` and ``metric-generator`` set."""
    reg = registry if registry is not None else load_metrics_registry()
    metrics = reg.get("metrics")
    if not isinstance(metrics, dict):
        return 0
    return sum(1 for entry in metrics.values() if is_fully_defined_metric(entry))


def get_kpi_automation_pct(*, registry: dict[str, Any] | None = None) -> int:
    """Value for LeanDNA **KPI Automation %**: fully-defined entries in ``my-metrics.yaml``."""
    return count_fully_defined_metrics(registry=registry)
