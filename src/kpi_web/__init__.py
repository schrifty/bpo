"""KPI web view: thin HTTP API + browser UI over ``kpi_service`` / registry.

Catalog CRUD forms are workstream D (not implemented here). Source of truth
remains ``config/my-metrics.yaml`` + SQLite KPI store — not a second store.
"""

from __future__ import annotations

from .app import create_app

__all__ = ["create_app"]
