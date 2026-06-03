"""Load and query ``config/metrics.yaml`` (LeanDNA metric registry)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .config_paths import METRICS_FILE

KPI_AUTOMATION_METRIC_NAME = "KPI Automation %"


def load_metrics_registry(*, path: Path | None = None) -> dict[str, Any]:
    """Parse ``config/metrics.yaml`` and return the document root."""
    metrics_path = path or METRICS_FILE
    if not metrics_path.is_file():
        raise FileNotFoundError(f"Metrics registry not found: {metrics_path}")
    data = yaml.safe_load(metrics_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping at root of {metrics_path}")
    return data


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
    """Value for LeanDNA **KPI Automation %**: fully-defined entries in ``metrics.yaml``."""
    return count_fully_defined_metrics(registry=registry)
