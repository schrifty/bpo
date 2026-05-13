"""Opt-in live checks against LeanDNA Data API (read-only).

Default CI and ``pytest`` runs **skip** these tests. To exercise real HTTP:

1. Set ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``LEANDNA_DATA_API_COOKIE`` (see ``docs/SETUP/LEANDNA_SETUP.md``).
2. Run with integration flag::

     BPO_LEANDNA_DATA_API_INTEGRATION=1 pytest tests/test_integration_leandna_data_api.py -v

Uses ``GET /data/Metric`` via :func:`src.leandna_metrics_client.list_metric_definitions` — small
payload, same auth stack as item master / shortages / lean projects.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]


def _integration_enabled() -> bool:
    raw = (os.environ.get("BPO_LEANDNA_DATA_API_INTEGRATION") or "").strip().lower()
    return raw in ("1", "true", "yes")


@pytest.mark.leandna_data_api
@pytest.mark.skipif(
    not _integration_enabled(),
    reason="Set BPO_LEANDNA_DATA_API_INTEGRATION=1 to run live LeanDNA Data API tests",
)
def test_list_metric_definitions_live() -> None:
    """GET /data/Metric succeeds with configured credentials."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        pytest.skip("dotenv not installed")
    load_dotenv(_ROOT / ".env", override=False)

    # conftest (and plugins) may import ``src.config`` before ``load_dotenv`` runs; config
    # snapshots env at import time. Reload so LEANDNA_* from .env are visible to HTTP clients.
    import src.config as _config

    importlib.reload(_config)
    import src.leandna_data_api_http as _ld_http

    importlib.reload(_ld_http)
    import src.leandna_metrics_client as _ld_metrics

    importlib.reload(_ld_metrics)
    leandna_data_api_credentials_configured = _ld_http.leandna_data_api_credentials_configured
    list_metric_definitions = _ld_metrics.list_metric_definitions

    if not leandna_data_api_credentials_configured():
        pytest.skip(
            "LeanDNA Data API credentials missing — set LEANDNA_DATA_API_BEARER_TOKEN "
            "and/or LEANDNA_DATA_API_COOKIE"
        )

    rows = list_metric_definitions(timeout_seconds=90.0)
    assert isinstance(rows, list)
    # Tenants may return an empty catalog; HTTP 200 + parseable body is the contract we assert.
    for row in rows[:5]:
        assert isinstance(row, dict)
