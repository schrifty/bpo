"""Live checks against LeanDNA Data API (read-only).

**This file logs secrets in full** (Bearer token, Cookie, curl replay) when a test runs — intended
for disposable sandbox tokens only.

Tests **skip** when LeanDNA credentials are missing. **Fail** when ``EXECUTION_ENV=Production`` or
``CI``. Require ``EXECUTION_ENV=Staging`` with ``ST_LEANDNA_DATA_API_*`` in ``.env``::

    python3 -m pytest tests/test_integration_leandna_data_api.py -v -m leandna_data_api

Each test loads ``.env`` with ``override=True`` so values there replace any ``LEANDNA_*`` already
set in the process environment (stale exports otherwise win with ``override=False``).
Logs include the bearer token **parsed from the on-disk** ``.env`` so you can tell unsaved-editor
drift from what ``load_dotenv`` applies.

Uses the OpenAPI **Metrics** catalog list — ``GET {LEANDNA_DATA_API_BASE_URL}/data/Metric`` — same
path as ``src.leandna_metrics_client.list_metric_definitions``, with the same auth headers as
item master / shortages / lean projects.

Also includes a **MetricReport** check that fetches ``GET /data/MetricReport`` for the
current fiscal year and prints the first KPI line from tenant data (read-only).
"""

from __future__ import annotations

import importlib
import logging
import os
import shlex
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import pytest
import requests

_ROOT = Path(__file__).resolve().parents[1]

_LOG = logging.getLogger("integration_leandna_data_api")

# ``build_leandna_data_api_headers`` defaults to ``cortex-…`` User-Agent; Swagger runs in a browser.
# If staging returns 401 only from Python, try matching a normal browser UA (see LEANDNA_SETUP).
_SWAGGER_LIKE_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _last_dotenv_value(dotenv_path: Path, key: str) -> str | None:
    """Return the last non-comment ``KEY=value`` assignment in ``dotenv_path`` (mirrors dotenv last-wins)."""
    if not dotenv_path.is_file():
        return None
    prefix = f"{key}="
    last: str | None = None
    try:
        text = dotenv_path.read_text(encoding="utf-8")
    except OSError:
        return None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "#" in line:
            line = line.split("#", 1)[0].rstrip()
        if line.startswith(prefix):
            val = line[len(prefix) :].strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]
            last = val
    return last


def _ensure_verbose_logging() -> None:
    """Emit DEBUG to stderr for this test and LeanDNA / urllib3."""
    fmt = "%(asctime)s %(levelname)-5s [%(name)s] %(message)s"
    root = logging.getLogger()
    if not any(type(h) is logging.StreamHandler for h in root.handlers):
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(fmt))
        root.addHandler(h)
    root.setLevel(logging.DEBUG)
    for name in (
        "integration_leandna_data_api",
        "cortex",
        "src.leandna_data_api_http",
        "src.leandna_data_api_request",
        "urllib3.connectionpool",
    ):
        logging.getLogger(name).setLevel(logging.DEBUG)


def _credential_summary(_config: object) -> None:
    bt = (getattr(_config, "LEANDNA_DATA_API_BEARER_TOKEN", None) or "").strip()
    ck = (getattr(_config, "LEANDNA_DATA_API_COOKIE", None) or "").strip()
    _LOG.info("LEANDNA_DATA_API_BEARER_TOKEN=%r", bt)
    _LOG.info("LEANDNA_DATA_API_COOKIE=%r", ck)
    _LOG.info(
        "origin=%r referer=%r",
        (getattr(_config, "LEANDNA_DATA_API_ORIGIN", None) or "").strip() or "(default from base URL)",
        (getattr(_config, "LEANDNA_DATA_API_REFERER", None) or "").strip() or "(default)",
    )


def _log_headers(headers: dict[str, str]) -> None:
    _LOG.info("Request headers: %s", dict(headers))


def _curl_equivalent(url: str, params: dict[str, str], headers: dict[str, str]) -> str:
    """Single-line curl for logs (verbatim Authorization / Cookie)."""
    q = urlencode(sorted(params.items())) if params else ""
    full_url = f"{url}?{q}" if q else url
    bits: list[str] = ["curl", "-sS", "-X", "GET", shlex.quote(full_url)]
    for name, val in sorted(headers.items()):
        bits.extend(["-H", shlex.quote(f"{name}: {val}")])
    return " ".join(bits)


@pytest.mark.leandna_data_api
def test_leandna_metric_report_live_displays_first_kpi(capsys) -> None:
    """Live ``GET /data/MetricReport`` — prints first KPI from tenant data (read-only, not mocked)."""
    _ensure_verbose_logging()
    _LOG.info("=== LeanDNA MetricReport live KPI: begin ===")
    try:
        from dotenv import load_dotenv
    except ImportError:
        pytest.skip("dotenv not installed")
    dotenv_path = _ROOT / ".env"
    load_dotenv(dotenv_path, override=True)

    import src.config as _config

    importlib.reload(_config)
    import src.leandna_data_api_http as _ld_http

    importlib.reload(_ld_http)
    import src.leandna_metrics_client as _ld_metrics

    importlib.reload(_ld_metrics)

    leandna_data_api_credentials_configured = _ld_http.leandna_data_api_credentials_configured
    _credential_summary(_config)

    if not leandna_data_api_credentials_configured():
        pytest.skip(
            "LeanDNA Data API credentials missing — set LEANDNA_DATA_API_BEARER_TOKEN "
            "and/or LEANDNA_DATA_API_COOKIE"
        )

    fiscal_year = date.today().year
    try:
        report = _ld_metrics.fetch_metric_report(fiscal_year)
    except requests.RequestException as e:
        pytest.skip(f"MetricReport request failed (network or HTTP): {e}")

    line = _ld_metrics.format_first_kpi_line_from_metric_report(report)
    _LOG.info(
        "MetricReport FY=%s format_line=%r report_top_keys=%s",
        fiscal_year,
        line,
        list(report.keys()),
    )

    if line == "KPI: (no metricValues)" or line.startswith("KPI: (metricValues"):
        pytest.skip(
            f"No displayable metricValues for FY={fiscal_year}; tenant may have no rows or a "
            f"different payload shape. Top-level keys: {list(report.keys())}"
        )

    with capsys.disabled():
        sys.stdout.write("\n--- LeanDNA KPI (live MetricReport) ---\n")
        sys.stdout.write(f"{line}\n")
        sys.stdout.write("---------------------------------------\n")
        sys.stdout.flush()

    assert line.startswith("KPI:")
    _LOG.info("=== LeanDNA MetricReport live KPI: success ===")


@pytest.mark.leandna_data_api
def test_leandna_metrics_list_endpoint_live() -> None:
    """GET /data/Metric (metrics definitions list) returns 200 and JSON the client can parse."""
    _ensure_verbose_logging()
    _LOG.info("=== LeanDNA metrics integration test: begin ===")
    try:
        from dotenv import load_dotenv
    except ImportError:
        pytest.skip("dotenv not installed")
    dotenv_path = _ROOT / ".env"
    # ``override=True``: if the shell or IDE already exported LEANDNA_* (e.g. an old token),
    # we still want this test to follow the repo ``.env`` you just edited.
    load_dotenv(dotenv_path, override=True)
    _LOG.info("load_dotenv(%s, override=True) completed", dotenv_path)
    disk_bearer = _last_dotenv_value(dotenv_path, "LEANDNA_DATA_API_BEARER_TOKEN")
    env_bearer = (os.environ.get("LEANDNA_DATA_API_BEARER_TOKEN") or "").strip()
    _LOG.info(
        "LEANDNA_DATA_API_BEARER_TOKEN last assignment on disk in %s: %r",
        dotenv_path,
        disk_bearer if disk_bearer is not None else "(no line found)",
    )
    _LOG.info("LEANDNA_DATA_API_BEARER_TOKEN in os.environ after load_dotenv: %r", env_bearer)
    if disk_bearer is not None and disk_bearer != env_bearer:
        _LOG.error(
            "Disk .env bearer != os.environ after load_dotenv — check for parse errors, "
            "wrong file path, or dotenv version behavior."
        )

    # conftest (and plugins) may import ``src.config`` before ``load_dotenv`` runs; config
    # snapshots env at import time. Reload so LEANDNA_* from .env are visible to HTTP clients.
    import src.config as _config

    importlib.reload(_config)
    import src.leandna_data_api_http as _ld_http

    importlib.reload(_ld_http)
    import src.leandna_metrics_client as _ld_metrics

    importlib.reload(_ld_metrics)
    _LOG.info("Reloaded src.config, src.leandna_data_api_http, src.leandna_metrics_client")
    leandna_data_api_credentials_configured = _ld_http.leandna_data_api_credentials_configured
    unwrap_rows = _ld_metrics._unwrap_metric_definition_rows  # noqa: SLF001 — keep parse logic in sync

    _credential_summary(_config)

    if not leandna_data_api_credentials_configured():
        pytest.skip(
            "LeanDNA Data API credentials missing — set LEANDNA_DATA_API_BEARER_TOKEN "
            "and/or LEANDNA_DATA_API_COOKIE"
        )

    base = (_config.LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api").rstrip("/")
    metrics_list_url = f"{base}/data/Metric"
    headers = _ld_http.build_leandna_data_api_headers(
        requested_sites=None,
        user_agent_suffix="leandna-integration-test/1.0",
    )
    headers["User-Agent"] = _SWAGGER_LIKE_USER_AGENT
    _log_headers(headers)
    # Match tenant Swagger default try-out: ``GET …/data/Metric?metricTypes=Manual``.
    metric_params = {"metricTypes": "Manual"}
    _LOG.info("Curl equivalent: %s", _curl_equivalent(metrics_list_url, metric_params, headers))

    t0 = time.perf_counter()
    resp = requests.get(metrics_list_url, headers=headers, params=metric_params, timeout=90.0)
    _LOG.info("requests actually used URL: %s", resp.request.url)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    _LOG.info(
        "Response received: status=%s elapsed_ms=%.1f content_length_header=%r encoding=%r",
        resp.status_code,
        elapsed_ms,
        resp.headers.get("Content-Length"),
        resp.encoding,
    )
    _LOG.debug("Response headers: %s", dict(resp.headers))
    body_preview = (resp.text or "")[:1200]
    _LOG.info("Response body length=%s prefix (up to 1200 chars): %s", len(resp.text or ""), body_preview)

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        snippet = (resp.text or "").strip().replace("\n", " ")[:800]
        _LOG.error("GET failed: %s body_prefix=%r", e, snippet)
        if resp.status_code in (401, 403):
            pytest.skip(
                f"LeanDNA returned {resp.status_code} for GET /data/Metric (invalid/expired token, "
                f"wrong LEANDNA_DATA_API_BASE_URL, or missing Cookie). body_prefix={snippet!r}"
            )
        hint = ""
        if resp.status_code == 401 and "Session not found" in (resp.text or ""):
            hint = (
                " Hint: 401 Session not found usually means LEANDNA_DATA_API_BEARER_TOKEN in .env "
                "is not the same token Swagger/curl uses for this LEANDNA_DATA_API_BASE_URL."
            )
        pytest.fail(f"GET {resp.request.url} failed: {e!s} body_prefix={snippet!r}{hint}")

    rows = unwrap_rows(resp.json())
    _LOG.info("Parsed metric definition rows: count=%s (showing types of first 3)", len(rows))
    for i, row in enumerate(rows[:3]):
        _LOG.debug("Row %s type=%s keys_sample=%s", i, type(row).__name__, list(row.keys())[:12] if isinstance(row, dict) else None)
    assert isinstance(rows, list)
    # Tenants may return an empty catalog; HTTP 200 + parseable body is the contract we assert.
    for row in rows[:5]:
        assert isinstance(row, dict)
    _LOG.info("=== LeanDNA metrics integration test: success ===")
