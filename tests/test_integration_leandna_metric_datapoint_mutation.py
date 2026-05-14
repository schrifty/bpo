"""Live integration: find metric **Data on-time rate** and show a sample datapoint value (read-only).

Uses ``GET /data/Metric`` (all metric types) and picks the first definition whose ``name`` /
``crossSiteName`` matches one of ``_TARGET_METRIC_SUBSTRINGS`` (normalized substring match),
tie-broken by lowest numeric ``id``, then ``GET /data/Metric/{id}/MetricDataPoint`` for the last
``_LOOKBACK_DAYS`` days.

**Credentials:** ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``LEANDNA_DATA_API_COOKIE`` in ``.env``.

Run::

    python3 -m pytest tests/test_integration_leandna_metric_datapoint_mutation.py -v
"""

from __future__ import annotations

import importlib
import sys
import unicodedata
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest
import requests

_ROOT = Path(__file__).resolve().parents[1]
_LOOKBACK_DAYS = 90
# Try in order until one matches ``name`` / ``crossSiteName`` (normalized substring).
_TARGET_METRIC_SUBSTRINGS: tuple[str, ...] = (
    "data on-time rate",
    "data on time rate",
    "data sync on time performance",
)


def _reload_leandna_clients():
    import src.config as _config

    importlib.reload(_config)
    import src.leandna_data_api_http as _ld_http

    importlib.reload(_ld_http)
    import src.leandna_data_api_request as _ld_req

    importlib.reload(_ld_req)
    import src.leandna_metrics_client as _ld_metrics

    importlib.reload(_ld_metrics)
    return _ld_http, _ld_req, _ld_metrics


def _unwrap_list_body(body: Any) -> list[Any]:
    if body is None:
        return []
    if isinstance(body, list):
        return body
    return []


def _normalize_match_text(s: str) -> str:
    """NFKC lowercase; hyphen/en/em dash → spaces; collapse whitespace."""
    t = unicodedata.normalize("NFKC", s or "")
    for ch in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"):
        t = t.replace(ch, " ")
    t = t.lower().replace("-", " ")
    return " ".join(t.split())


def _pick_metric_by_name_substring(rows: list[Any], needle: str) -> dict[str, Any] | None:
    """Return one metric dict whose name or crossSiteName contains ``needle`` (normalized).

    If several match, return the one with the smallest numeric ``id`` (stable choice).
    """
    n = _normalize_match_text(needle)
    if not n:
        return None
    defs = [r for r in rows if isinstance(r, dict) and r.get("id") is not None]
    if not defs:
        return None

    def sort_key(r: dict[str, Any]) -> int:
        raw = r.get("id")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    matches: list[dict[str, Any]] = []
    for d in defs:
        blob = _normalize_match_text(f"{d.get('name', '')} {d.get('crossSiteName', '')}")
        if n in blob:
            matches.append(d)
    if not matches:
        return None
    matches.sort(key=sort_key)
    return matches[0]


def _pick_metric_by_name_substrings(
    rows: list[Any], needles: tuple[str, ...]
) -> tuple[dict[str, Any] | None, str | None]:
    """First matching ``needles`` entry wins; return ``(metric_dict, matched_needle)``."""
    for needle in needles:
        m = _pick_metric_by_name_substring(rows, needle)
        if m is not None:
            return m, needle
    return None, None


def _hint_metric_names(rows: list[Any], *, limit: int = 25) -> list[str]:
    """Names that might be near-misses for a data / on-time KPI search."""
    names: list[str] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        n = str(r.get("name") or "").strip()
        if not n:
            continue
        low = n.lower()
        if ("on-time" in low or "on time" in low) and ("data" in low or "kpi" in low or "sync" in low):
            names.append(n)
    names.sort()
    return names[:limit]


def _format_datapoint_display_line(metric_name: str, metric_id: Any, row: dict[str, Any]) -> str:
    val = row.get("value")
    d = row.get("dataPointDate")
    vs = row.get("valueStreamId")
    cat = row.get("category")
    bits = [
        f'metric="{metric_name}"',
        f"id={metric_id}",
        f"dataPointDate={d!r}",
        f"valueStreamId={vs!r}",
    ]
    if cat is not None:
        bits.append(f"category={cat!r}")
    bits.append(f"value={val!r}")
    return "LeanDNA sample MetricDataPoint: " + ", ".join(bits)


@pytest.mark.leandna_data_api
def test_leandna_displays_single_metric_datapoint_value(capsys) -> None:
    """Find ``Data on-time rate`` in the catalog; print one datapoint from a recent window (read-only)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        pytest.skip("dotenv not installed")

    load_dotenv(_ROOT / ".env", override=True)
    _ld_http, _ld_req, _ld_metrics = _reload_leandna_clients()

    if not _ld_http.leandna_data_api_credentials_configured():
        pytest.skip(
            "LeanDNA credentials missing — set LEANDNA_DATA_API_BEARER_TOKEN and/or "
            "LEANDNA_DATA_API_COOKIE in .env"
        )

    try:
        catalog = _ld_metrics.list_metric_definitions(
            requested_sites=None,
            timeout_seconds=120.0,
            extra_query=None,
        )
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None) if e.response is not None else None
        if code in (401, 403):
            pytest.skip(f"LeanDNA GET /data/Metric returned HTTP {code}: {e}")
        raise

    chosen, matched_needle = _pick_metric_by_name_substrings(catalog, _TARGET_METRIC_SUBSTRINGS)
    if chosen is None:
        hints = _hint_metric_names(catalog)
        with capsys.disabled():
            sys.stdout.write(
                "OUTCOME: FAILURE — no catalog metric matched any of "
                f"{_TARGET_METRIC_SUBSTRINGS!r} (edit needles in this test file if the UI label differs)\n"
            )
            if hints:
                sys.stdout.write("Hint — similar metric names on this tenant:\n")
                for h in hints:
                    sys.stdout.write(f"  - {h}\n")
            sys.stdout.flush()
        pytest.fail(
            "GET /data/Metric: no metric with name/crossSiteName matching "
            f"{_TARGET_METRIC_SUBSTRINGS!r}"
        )

    metric_id = chosen["id"]
    metric_name = str(chosen.get("name") or chosen.get("crossSiteName") or metric_id)

    end_d = date.today()
    start_d = end_d - timedelta(days=_LOOKBACK_DAYS)
    start_s = start_d.isoformat()
    end_s = end_d.isoformat()

    env = _ld_req.data_api_get_json(
        f"Metric/{metric_id}/MetricDataPoint",
        query={"startDate": start_s, "endDate": end_s},
        requested_sites=None,
        user_agent_suffix="leandna-metric-display-integration/1.0",
        timeout_seconds=120.0,
    )
    if not env.get("ok"):
        st = env.get("status")
        if st in (401, 403):
            pytest.skip(f"LeanDNA GET MetricDataPoint returned HTTP {st}: {env!r}")
        with capsys.disabled():
            sys.stdout.write(f"OUTCOME: FAILURE — GET MetricDataPoint failed: {env!r}\n")
            sys.stdout.flush()
        pytest.fail(f"GET MetricDataPoint failed: {env!r}")

    points = _unwrap_list_body(env.get("body"))
    point_rows = [p for p in points if isinstance(p, dict)]

    if not point_rows:
        line = (
            f'LeanDNA metric "{metric_name}" (id={metric_id}): no MetricDataPoint rows between '
            f"{start_s} and {end_s}"
        )
        outcome_detail = (
            f"GET Metric + MetricDataPoint succeeded; zero rows in {_LOOKBACK_DAYS}d window "
            f"({start_s}..{end_s})"
        )
    else:
        point_rows.sort(key=lambda r: str(r.get("dataPointDate") or ""))
        line = _format_datapoint_display_line(metric_name, metric_id, point_rows[-1])
        outcome_detail = (
            f"GET Metric + MetricDataPoint succeeded; printed latest row in {_LOOKBACK_DAYS}d window"
        )

    outcome = "SUCCESS"

    with capsys.disabled():
        sys.stdout.write("\n--- LeanDNA metric display (integration) ---\n")
        sys.stdout.write(f"Catalog match: substring {matched_needle!r} → id={metric_id}\n")
        sys.stdout.write(line + "\n")
        sys.stdout.write("-------------------------------------------\n")
        sys.stdout.write(f"OUTCOME: {outcome} — {outcome_detail}\n")
        sys.stdout.flush()

    assert outcome == "SUCCESS"
