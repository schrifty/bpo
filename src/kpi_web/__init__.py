"""KPI web view + maintain: thin HTTP API + browser UI over registry / ``kpi_service``.

Catalog CRUD reuses ``metrics_registry_write`` (same YAML shape as the CLI).
Source of truth remains ``config/my-metrics.yaml`` + SQLite KPI store.
"""

from __future__ import annotations

from .app import create_app

__all__ = ["create_app"]
