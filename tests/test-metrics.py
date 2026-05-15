"""Live integration: chart metric **id 638** ``MetricDataPoint`` series, dump one sample row, **narrow GET → POST-if-missing → DELETE** for a fixed date.

Uses ``GET /data/Metric`` to resolve ``_TARGET_METRIC_ID``, ``GET …/MetricDataPoint`` for a window
that always includes ``_TARGET_POST_DATAPOINT_DATE`` (``end`` = ``max(today, post date)``,
``start`` = ``end`` − ``_LOOKBACK_DAYS``). Prints an ASCII chart, **every key/value** on the GET row
whose ``dataPointDate`` is ``_SAMPLE_DATAPOINT_DUMP_DATE`` for metric ``_TARGET_METRIC_ID`` (and JSON).
The **window** date/value series is printed once at the start and again **after each** Data API GET/POST/DELETE in the mutation sequence (no duplicate chart at the end).
A **narrow GET** (``startDate`` = ``endDate`` = ``_TARGET_POST_DATAPOINT_DATE``) runs **before POST**;
**POST** runs only when that date is absent. If POST runs, **any failure** fails the test. If the
row was already present, POST is skipped and **DELETE** still removes it. **DELETE** must succeed
(2xx); **404** is not treated as success here because a row is always expected before DELETE.

**Credentials:** ``LEANDNA_DATA_API_BEARER_TOKEN`` and/or ``LEANDNA_DATA_API_COOKIE`` in ``.env``.
``RequestedSites`` is set to the metric’s ``siteId`` when the catalog provides it (often required
for mutations). **POST JSON** matches the working Data API shape: ``dataPointDate``, ``metricId``
(catalog id, same as path), ``category``, ``value``, ``numeratorValue``, ``denominatorValue`` (no
``valueStreamId`` in the body). POST **403/401** fails the test with ``OUTCOME: FAILURE`` (no silent skip).

Run::

    python3 -m pytest tests/test-metrics.py -v
"""

from __future__ import annotations

import importlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest
import requests

_ROOT = Path(__file__).resolve().parents[1]
_LOOKBACK_DAYS = 90
_TARGET_METRIC_ID = 638
# Fixed row to add when missing (edit for your tenant).
_TARGET_POST_DATAPOINT_DATE = "2026-05-12"
_NEW_DATAPOINT_VALUE = 33
# Field dump: row with this ``dataPointDate`` from ``GET …/Metric/{_TARGET_METRIC_ID}/MetricDataPoint``.
_SAMPLE_DATAPOINT_DUMP_DATE = "2026-05-14"
# POST body ``denominatorValue`` (edit per tenant / metric semantics).
_POST_DENOMINATOR_VALUE = 1


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


def _pick_metric_by_id(rows: list[Any], metric_id: int) -> dict[str, Any] | None:
    for r in rows:
        if not isinstance(r, dict) or r.get("id") is None:
            continue
        try:
            if int(r["id"]) == metric_id:
                return r
        except (TypeError, ValueError):
            continue
    return None


def _extract_date_value_pairs(rows: list[dict[str, Any]]) -> list[tuple[str, Any, float | None]]:
    """``(dataPointDate, raw value, float value or None)`` per row (caller sorts by date)."""
    out: list[tuple[str, Any, float | None]] = []
    for r in rows:
        d = str(r.get("dataPointDate") or "").strip()
        raw = r.get("value")
        fv: float | None
        try:
            fv = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            fv = None
        out.append((d, raw, fv))
    return out


def _format_date_datapoint_chart(
    pairs: list[tuple[str, Any, float | None]],
    *,
    bar_width: int = 48,
    heading: str | None = "--- Chart: dataPointDate vs datapoint (value) ---",
) -> list[str]:
    """ASCII chart: one row per ``dataPointDate`` with ``value`` and a horizontal bar."""
    floats = [p[2] for p in pairs if p[2] is not None]
    lines: list[str] = []
    if heading is not None:
        lines.append(heading)
    if not pairs:
        lines.append("(no rows)")
        return lines
    if not floats:
        lines.append("dataPointDate    datapoint")
        lines.append("---------------+-------------")
        for d, raw, _ in pairs:
            lines.append(f"{d.ljust(15)} | {str(raw).rjust(11)}")
        lines.append("(values are non-numeric — no bar scale)")
        return lines

    vmin, vmax = min(floats), max(floats)
    span = vmax - vmin
    lines.append("dataPointDate    datapoint     chart")
    lines.append("---------------+-------------+-" + ("-" * bar_width))
    if span <= 0:
        bar_len = max(1, min(bar_width, bar_width // 2))
        for d, raw, _fv in pairs:
            bar = "█" * bar_len
            lines.append(f"{d.ljust(15)} | {str(raw).rjust(11)} | {bar}")
        lines.append(f"(flat series in window: value = {vmin})")
        return lines

    for d, raw, fv in pairs:
        if fv is None:
            bar = "(n/a)"
        else:
            frac = (fv - vmin) / span
            n = max(1, min(bar_width, int(round(frac * bar_width)) or 1))
            bar = "█" * n
        lines.append(f"{d.ljust(15)} | {str(raw).rjust(11)} | {bar}")
    lines.append(f"(bars scaled from min={vmin} to max={vmax} in this window)")
    return lines


def _window_series_chart_lines(
    _ld_req: Any,
    metric_id: Any,
    start_s: str,
    end_s: str,
    *,
    user_agent_suffix: str = "leandna-metric-display-integration/1.0",
    timeout_seconds: float = 120.0,
) -> list[str]:
    """Re-fetch the lookback window and return chart lines (``heading=None``)."""
    env = _ld_req.data_api_get_json(
        f"Metric/{metric_id}/MetricDataPoint",
        query={"startDate": start_s, "endDate": end_s},
        requested_sites=None,
        user_agent_suffix=user_agent_suffix,
        timeout_seconds=timeout_seconds,
    )
    if not env.get("ok"):
        return [
            f"(cannot chart window: GET HTTP {env.get('status')} {env.get('error')!r})",
        ]
    rows = [p for p in _unwrap_list_body(env.get("body")) if isinstance(p, dict)]
    rows.sort(key=lambda r: str(r.get("dataPointDate") or ""))
    pairs = _extract_date_value_pairs(rows)
    return _format_date_datapoint_chart(pairs, heading=None)


def _emit_data_series(capsys: Any, phase: str, chart_lines: list[str]) -> None:
    with capsys.disabled():
        sys.stdout.write(f"\n=== Data series — {phase} ===\n")
        for ln in chart_lines:
            sys.stdout.write(ln + "\n")
        sys.stdout.flush()


def _stdout_post_lines_since(capsys: Any, post_print: list[str], start: int) -> int:
    """Print ``post_print[start:]``; return next index (``len(post_print)`` if anything printed)."""
    if start >= len(post_print):
        return start
    with capsys.disabled():
        for i in range(start, len(post_print)):
            sys.stdout.write(post_print[i] + "\n")
        sys.stdout.flush()
    return len(post_print)


def _requested_sites_for_metric(chosen: dict[str, Any]) -> str | None:
    sid = chosen.get("siteId")
    if sid is None:
        return None
    s = str(sid).strip()
    return s or None


def _build_metric_datapoint_post_body(
    *,
    data_point_date_iso: str,
    metric_id: int,
    template_row: dict[str, Any] | None,
    new_value: float | int,
) -> dict[str, Any]:
    """Body aligned with LeanDNA ``POST /data/Metric/{{id}}/MetricDataPoint`` (catalog ``metricId``)."""
    cat = ""
    if template_row:
        cat = str(template_row.get("category") or "")
    return {
        "dataPointDate": data_point_date_iso,
        "metricId": metric_id,
        "category": cat,
        "value": new_value,
        "numeratorValue": new_value,
        "denominatorValue": _POST_DENOMINATOR_VALUE,
    }


def _datapoint_dates(rows: list[dict[str, Any]]) -> set[str]:
    return {str(r.get("dataPointDate") or "").strip() for r in rows if isinstance(r, dict)}


def _pick_datapoint_row_for_date(rows: list[dict[str, Any]], date_iso: str) -> dict[str, Any] | None:
    """First row whose ``dataPointDate`` matches ``date_iso`` (after normal strip)."""
    want = (date_iso or "").strip()
    for r in rows:
        if not isinstance(r, dict):
            continue
        if str(r.get("dataPointDate") or "").strip() == want:
            return r
    return None


def _format_metric_datapoint_all_fields(
    row: dict[str, Any],
    *,
    metric_catalog_id: Any,
    data_point_date: str,
) -> list[str]:
    """List every key/value on one ``MetricDataPoint`` object from ``GET …/MetricDataPoint``."""
    lines = [
        f"--- Sample MetricDataPoint (GET …/Metric/{metric_catalog_id}/MetricDataPoint; "
        f"dataPointDate={data_point_date!r}) — all fields ---",
    ]
    for k in sorted(row.keys()):
        lines.append(f"  {k}: {row[k]!r}")
    lines.append("--- Same object as JSON (sorted keys) ---")
    lines.append(json.dumps(dict(sorted(row.items())), indent=2, default=str, ensure_ascii=False))
    return lines


@pytest.mark.leandna_data_api
def test_leandna_displays_single_metric_datapoint_value(capsys) -> None:
    """Chart metric id 638; narrow GET then POST-if-missing (POST strict); DELETE; verify removal."""
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

    chosen = _pick_metric_by_id(catalog, _TARGET_METRIC_ID)
    if chosen is None:
        with capsys.disabled():
            sys.stdout.write(
                f"OUTCOME: FAILURE — catalog has no metric definition with id={_TARGET_METRIC_ID}\n"
            )
            sys.stdout.flush()
        pytest.fail(f"GET /data/Metric: no metric with id={_TARGET_METRIC_ID}")

    metric_id = chosen["id"]
    metric_name = str(chosen.get("name") or chosen.get("crossSiteName") or metric_id)

    post_d = date.fromisoformat(_TARGET_POST_DATAPOINT_DATE)
    end_d = max(date.today(), post_d)
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

    sample_point_lines: list[str] = []

    if not point_rows:
        sample_point_lines = [
            "--- Sample MetricDataPoint (all fields from GET) ---",
            f"(no rows in window {start_s!r}..{end_s!r} — cannot dump {_SAMPLE_DATAPOINT_DUMP_DATE!r} for "
            f"metric id={_TARGET_METRIC_ID})",
        ]
        outcome_detail = (
            f"GET Metric + MetricDataPoint succeeded; zero rows in {_LOOKBACK_DAYS}d window "
            f"({start_s}..{end_s})"
        )
        beginning_pairs: list[tuple[str, Any, float | None]] = []
    else:
        point_rows.sort(key=lambda r: str(r.get("dataPointDate") or ""))
        sample_row = _pick_datapoint_row_for_date(point_rows, _SAMPLE_DATAPOINT_DUMP_DATE)
        if sample_row is None:
            sample_point_lines = [
                f"--- Sample MetricDataPoint (metric id={metric_id}, "
                f"dataPointDate={_SAMPLE_DATAPOINT_DUMP_DATE!r}) ---",
                f"(no GET row for that date in window {start_s!r}..{end_s!r})",
            ]
        else:
            sample_point_lines = _format_metric_datapoint_all_fields(
                sample_row,
                metric_catalog_id=metric_id,
                data_point_date=_SAMPLE_DATAPOINT_DUMP_DATE,
            )
        n = len(point_rows)
        outcome_detail = (
            f"GET Metric + MetricDataPoint succeeded ({n} row(s)) in {_LOOKBACK_DAYS}d window "
            f"({start_s}..{end_s})"
        )
        beginning_pairs = _extract_date_value_pairs(point_rows)

    beginning_chart = _format_date_datapoint_chart(beginning_pairs, heading=None)
    meta_line = (
        f'LeanDNA metric "{metric_name}" (id={metric_id})  '
        f"window {start_s!r}..{end_s!r}"
        + (f"  ({len(point_rows)} datapoint row(s))" if point_rows else "  (no datapoint rows)")
    )

    with capsys.disabled():
        sys.stdout.write("\n--- LeanDNA metric display (integration) ---\n")
        sys.stdout.write(f"Catalog match: metric id={metric_id} name={metric_name!r}\n")
        sys.stdout.write(meta_line + "\n")
        sys.stdout.flush()
    _emit_data_series(capsys, "beginning (after GET window)", beginning_chart)
    with capsys.disabled():
        for spl in sample_point_lines:
            sys.stdout.write(spl + "\n")
        sys.stdout.flush()
    post_iso = _TARGET_POST_DATAPOINT_DATE
    req_sites = _requested_sites_for_metric(chosen)
    post_print: list[str] = []
    post_i = 0

    post_print.append(
        f"--- Narrow GET before POST (startDate=endDate={post_iso!r}) — row already there? ---"
    )
    env_pre = _ld_req.data_api_get_json(
        f"Metric/{metric_id}/MetricDataPoint",
        query={"startDate": post_iso, "endDate": post_iso},
        requested_sites=None,
        user_agent_suffix="leandna-metric-display-integration/1.0",
        timeout_seconds=120.0,
    )
    if not env_pre.get("ok"):
        st = env_pre.get("status")
        if st in (401, 403):
            pytest.skip(f"LeanDNA narrow GET MetricDataPoint returned HTTP {st}: {env_pre!r}")
        with capsys.disabled():
            sys.stdout.write("\n--- LeanDNA metric display (integration) ---\n")
            sys.stdout.write(f"Catalog match: metric id={metric_id} name={metric_name!r}\n")
            sys.stdout.write(meta_line + "\n")
            for spl in sample_point_lines:
                sys.stdout.write(spl + "\n")
            for pl in post_print:
                sys.stdout.write(pl + "\n")
            sys.stdout.flush()
        _emit_data_series(
            capsys,
            "current window (at failure)",
            _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
        )
        with capsys.disabled():
            sys.stdout.write(f"OUTCOME: FAILURE — narrow GET before POST failed: {env_pre!r}\n")
            sys.stdout.flush()
        pytest.fail(f"narrow GET MetricDataPoint before POST failed: {env_pre!r}")

    pre_rows = [p for p in _unwrap_list_body(env_pre.get("body")) if isinstance(p, dict)]
    row_present_before_post = post_iso in _datapoint_dates(pre_rows)
    post_print.append(
        f"Narrow GET before POST: {len(pre_rows)} row(s); "
        f"dataPointDate {post_iso!r} present = {row_present_before_post}"
    )
    post_i = _stdout_post_lines_since(capsys, post_print, post_i)
    _emit_data_series(
        capsys,
        f"narrow GET (pre-POST) startDate=endDate={post_iso!r}",
        _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
    )

    template = point_rows[-1] if point_rows else None
    try:
        metric_id_int = int(metric_id)
    except (TypeError, ValueError) as e:
        pytest.fail(f"metric id is not an integer: {metric_id!r} ({e})")
    post_body = _build_metric_datapoint_post_body(
        data_point_date_iso=post_iso,
        metric_id=metric_id_int,
        template_row=template,
        new_value=_NEW_DATAPOINT_VALUE,
    )

    post_skipped = False

    if row_present_before_post:
        post_skipped = True
        post_print.append(
            f"--- POST MetricDataPoint (skipped — row already exists for {post_iso!r}) ---"
        )
    else:
        post_print.append(f"--- POST MetricDataPoint (insert dataPointDate={post_iso}) ---")
        post_print.append(f"RequestedSites={req_sites!r}")
        post_print.append(f"JSON body: {post_body!r}")
        post_env = _ld_req.data_api_mutate_json(
            "POST",
            f"Metric/{metric_id}/MetricDataPoint",
            json_body=post_body,
            requested_sites=req_sites,
            user_agent_suffix="leandna-metric-display-integration/1.0",
            timeout_seconds=120.0,
        )
        if not post_env.get("ok"):
            pst = post_env.get("status")
            prev = (post_env.get("body_preview") or post_env.get("error") or "")[:400]
            post_print.append(f"POST failed: HTTP {pst} preview={prev!r}")
            with capsys.disabled():
                sys.stdout.write("\n--- LeanDNA metric display (integration) ---\n")
                sys.stdout.write(f"Catalog match: metric id={metric_id} name={metric_name!r}\n")
                sys.stdout.write(meta_line + "\n")
                for spl in sample_point_lines:
                    sys.stdout.write(spl + "\n")
                for pl in post_print:
                    sys.stdout.write(pl + "\n")
                sys.stdout.flush()
            _emit_data_series(
                capsys,
                "current window (at failure)",
                _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
            )
            with capsys.disabled():
                sys.stdout.write(f"OUTCOME: FAILURE — POST MetricDataPoint failed: {post_env!r}\n")
                sys.stdout.flush()
            pytest.fail(f"POST MetricDataPoint failed: {post_env!r}")
        post_print.append(f"POST OK HTTP {post_env.get('status')}")

    post_i = _stdout_post_lines_since(capsys, post_print, post_i)
    _emit_data_series(
        capsys,
        "POST (skipped — no REST)" if post_skipped else "POST MetricDataPoint",
        _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
    )

    post_print.append(
        f"--- DELETE MetricDataPoint (query startDate=endDate={post_iso!r}) ---"
    )
    del_env = _ld_req.data_api_mutate_json(
        "DELETE",
        f"Metric/{metric_id}/MetricDataPoint",
        query={"startDate": post_iso, "endDate": post_iso},
        requested_sites=req_sites,
        user_agent_suffix="leandna-metric-display-integration/1.0",
        timeout_seconds=120.0,
    )
    delete_ok = bool(del_env.get("ok"))
    if not delete_ok:
        dst = del_env.get("status")
        with capsys.disabled():
            sys.stdout.write("\n--- LeanDNA metric display (integration) ---\n")
            sys.stdout.write(f"Catalog match: metric id={metric_id} name={metric_name!r}\n")
            sys.stdout.write(meta_line + "\n")
            for spl in sample_point_lines:
                sys.stdout.write(spl + "\n")
            for pl in post_print:
                sys.stdout.write(pl + "\n")
            sys.stdout.flush()
        _emit_data_series(
            capsys,
            "current window (at failure)",
            _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
        )
        with capsys.disabled():
            sys.stdout.write(
                f"OUTCOME: FAILURE — DELETE MetricDataPoint returned HTTP {dst}: {del_env!r}\n"
            )
            sys.stdout.flush()
        pytest.fail(f"DELETE MetricDataPoint failed: {del_env!r}")

    post_print.append(f"DELETE OK HTTP {del_env.get('status')}")
    if del_env.get("body") is not None:
        post_print.append(f"DELETE response body: {del_env.get('body')!r}")

    post_i = _stdout_post_lines_since(capsys, post_print, post_i)
    _emit_data_series(
        capsys,
        "DELETE MetricDataPoint",
        _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
    )

    env_verify = _ld_req.data_api_get_json(
        f"Metric/{metric_id}/MetricDataPoint",
        query={"startDate": post_iso, "endDate": post_iso},
        requested_sites=None,
        user_agent_suffix="leandna-metric-display-integration/1.0",
        timeout_seconds=120.0,
    )
    if env_verify.get("ok"):
        verify_rows = [p for p in _unwrap_list_body(env_verify.get("body")) if isinstance(p, dict)]
        post_print.append(
            f"Verify GET only {post_iso!r}: {len(verify_rows)} row(s) (expect 0 after DELETE)"
        )
        if post_iso in _datapoint_dates(verify_rows):
            with capsys.disabled():
                sys.stdout.write("\n--- LeanDNA metric display (integration) ---\n")
                sys.stdout.write(f"Catalog match: metric id={metric_id} name={metric_name!r}\n")
                sys.stdout.write(meta_line + "\n")
                for spl in sample_point_lines:
                    sys.stdout.write(spl + "\n")
                for pl in post_print:
                    sys.stdout.write(pl + "\n")
                sys.stdout.flush()
            _emit_data_series(
                capsys,
                "current window (at failure)",
                _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
            )
            with capsys.disabled():
                sys.stdout.write(
                    "OUTCOME: FAILURE — DELETE succeeded but GET still returns that date\n"
                )
                sys.stdout.flush()
            pytest.fail(f"DELETE did not remove dataPointDate={post_iso!r} (GET still has rows)")
    else:
        post_print.append(f"Verify GET narrow window not ok (non-fatal): {env_verify!r}")

    post_i = _stdout_post_lines_since(capsys, post_print, post_i)
    _emit_data_series(
        capsys,
        f"GET verify (narrow startDate=endDate={post_iso!r})",
        _window_series_chart_lines(_ld_req, metric_id, start_s, end_s),
    )

    post_summary = (
        " POST+DELETE cycle: "
        + ("POST skipped (already present); " if post_skipped else "POST ok; ")
        + "DELETE ok."
    )

    outcome_detail += post_summary
    outcome = "SUCCESS"

    post_i = _stdout_post_lines_since(capsys, post_print, post_i)
    with capsys.disabled():
        sys.stdout.write("-------------------------------------------\n")
        sys.stdout.write(f"OUTCOME: {outcome} — {outcome_detail}\n")
        sys.stdout.flush()

    assert outcome == "SUCCESS"
