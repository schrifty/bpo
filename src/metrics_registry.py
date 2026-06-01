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


def is_fully_defined_metric(entry: Any) -> bool:
    """True when *entry* has ``metric-id`` and a non-empty ``metric-generator``."""
    if not isinstance(entry, dict):
        return False
    mid = entry.get("metric-id")
    if mid is None or str(mid).strip() == "":
        return False
    gen = entry.get("metric-generator")
    if gen is None:
        return False
    if isinstance(gen, str) and not gen.strip():
        return False
    return True


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
