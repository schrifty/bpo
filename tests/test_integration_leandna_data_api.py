"""Opt-in live checks against LeanDNA Data API (read-only).

Default CI and ``pytest`` runs **skip** these tests. To exercise real HTTP:

1. Set ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``LEANDNA_DATA_API_COOKIE`` (see ``docs/SETUP/LEANDNA_SETUP.md``).
2. Run with integration flag::

     BPO_LEANDNA_DATA_API_INTEGRATION=1 pytest tests/test_integration_leandna_data_api.py -v

Uses the OpenAPI **Metrics** catalog list — ``GET {LEANDNA_DATA_API_BASE_URL}/data/Metric`` — same
path as :func:`src.leandna_metrics_client.list_metric_definitions`, with the same auth headers as
item master / shortages / lean projects.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest
import requests

_ROOT = Path(__file__).resolve().parents[1]


def _integration_enabled() -> bool:
    raw = (os.environ.get("BPO_LEANDNA_DATA_API_INTEGRATION") or "").strip().lower()
    return raw in ("1", "true", "yes")


@pytest.mark.leandna_data_api
@pytest.mark.skipif(
    not _integration_enabled(),
    reason="Set BPO_LEANDNA_DATA_API_INTEGRATION=1 to run live LeanDNA Data API tests",
)
def test_leandna_metrics_list_endpoint_live() -> None:
    """GET /data/Metric (metrics definitions list) returns 200 and JSON the client can parse."""
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
    unwrap_rows = _ld_metrics._unwrap_metric_definition_rows  # noqa: SLF001 — keep parse logic in sync

    if not leandna_data_api_credentials_configured():
        pytest.skip(
            "LeanDNA Data API credentials missing — set LEANDNA_DATA_API_BEARER_TOKEN "
            "and/or LEANDNA_DATA_API_COOKIE"
        )

    base = (_config.LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api").rstrip("/")
    metrics_list_url = f"{base}/data/Metric"
    resp = requests.get(
        metrics_list_url,
        headers=_ld_http.build_leandna_data_api_headers(
            requested_sites=None,
            user_agent_suffix="leandna-integration-test/1.0",
        ),
        timeout=90.0,
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        snippet = (resp.text or "").strip().replace("\n", " ")[:500]
        pytest.fail(f"GET {metrics_list_url} failed: {e!s} body_prefix={snippet!r}")

    rows = unwrap_rows(resp.json())
    assert isinstance(rows, list)
    # Tenants may return an empty catalog; HTTP 200 + parseable body is the contract we assert.
    for row in rows[:5]:
        assert isinstance(row, dict)
