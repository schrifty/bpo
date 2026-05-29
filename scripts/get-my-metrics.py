#!/usr/bin/env python3
"""List LeanDNA metrics assigned to the **current user**.

Configuration is from ``.env`` (``EXECUTION_ENV``, ``LEANDNA_APP_METRIC_OWNER``, etc.).
Optional ``--values``: ASCII chart of the last 10 datapoints per metric.

Output: auth line, full JSON payload, and (with ``--values``) per-KPI charts.

Run::

  get-my-metrics
  get-my-metrics --values
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

import requests  # noqa: E402

from src.config import (  # noqa: E402
    BPO_LEANDNA_DATA_API_EXECUTION_BUCKET,
    LEANDNA_APP_FACTORY_NDX,
    LEANDNA_APP_USER_NDX,
)
from src.leandna_app_metrics_client import (  # noqa: E402
    fetch_metric_entries_range,
    list_my_metrics_view,
    metric_view_label,
    resolve_app_metric_owner,
)
from src.leandna_app_metrics_http import leandna_app_session_configured  # noqa: E402
from src.leandna_metrics_display import print_metric_value_chart  # noqa: E402

_READ_TIMEOUT_S = 60.0
_VALUES_LOOKBACK_DAYS = 120
_VALUES_POINT_COUNT = 10


def _sort_rows(rows: list[dict]) -> list[dict]:
    def key(r: dict) -> tuple:
        raw = r.get("ndx", r.get("id"))
        try:
            return (0, int(raw))
        except (TypeError, ValueError):
            return (1, str(raw or ""))

    return sorted(rows, key=key)


def _auth_banner(*, source: str, owner_label: str, owner: str, kind: str) -> str:
    bucket = BPO_LEANDNA_DATA_API_EXECUTION_BUCKET
    if source == "app":
        return (
            f"Auth: LeanDNA app API (Metrics/View, metricOwner={owner!r}, filter={kind}, "
            f"EXECUTION_ENV={bucket})"
        )
    return (
        f"Auth: LeanDNA Data API (/data/identity + /data/Metric, owner={owner_label!r}, "
        f"EXECUTION_ENV={bucket})"
    )


def _build_payload(
    *,
    source: str,
    owner_label: str,
    rows: list[dict],
    owner: str,
    kind: str,
    identity: dict[str, Any] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": source,
        "ownerLabel": owner_label,
        "metrics": rows,
    }
    if source == "app":
        payload["metricOwner"] = owner
        payload["ownerFilterKind"] = kind
    if identity:
        payload["identity"] = identity
    return payload


def _metric_id(metric: dict[str, Any]) -> Any:
    return metric.get("ndx", metric.get("id"))


def _requested_sites(metric: dict[str, Any]) -> str | None:
    sid = metric.get("siteId")
    if sid is None:
        return None
    s = str(sid).strip()
    return s or None


def _value_stream_ndx(metric: dict[str, Any]) -> int | None:
    raw = metric.get("valueStreamNdx")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _fetch_datapoints_data_api(metric: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    from src.leandna_data_api_http import leandna_data_api_credentials_configured
    from src.leandna_metrics_client import (
        fetch_metric_datapoints,
        resolve_metric_datapoint_window,
        slim_metric_datapoint_rows,
    )

    if not leandna_data_api_credentials_configured():
        return [], "Data API not configured"
    mid = _metric_id(metric)
    if mid is None:
        return [], "metric has no id"
    start_s, end_s = resolve_metric_datapoint_window(lookback_days=_VALUES_LOOKBACK_DAYS)
    rows, err = fetch_metric_datapoints(
        mid,
        start_date=start_s,
        end_date=end_s,
        requested_sites=_requested_sites(metric),
        timeout_seconds=_READ_TIMEOUT_S,
    )
    if err:
        return [], str(err.get("error") or err)
    return slim_metric_datapoint_rows(rows), None


def _fetch_datapoints_app_api(metric: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    from src.leandna_metrics_client import resolve_metric_datapoint_window

    mid = _metric_id(metric)
    try:
        ndx_i = int(mid)
    except (TypeError, ValueError):
        return [], f"invalid metric id {mid!r}"
    start_s, end_s = resolve_metric_datapoint_window(lookback_days=_VALUES_LOOKBACK_DAYS)
    points, err = fetch_metric_entries_range(
        ndx_i,
        start_date=start_s,
        end_date=end_s,
        value_stream_ndx=_value_stream_ndx(metric),
        factory_ndx=LEANDNA_APP_FACTORY_NDX,
        switch_site_first=False,
        timeout_per_day=_READ_TIMEOUT_S,
    )
    if err:
        return [], str(err.get("error") or err)
    slim = [{"dataPointDate": p.get("dataPointDate"), "value": p.get("value")} for p in points]
    return slim, None


def _fetch_last_datapoints(metric: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    """Datapoints for charting; Data API when configured, else app MetricEntries."""
    from src.leandna_data_api_http import leandna_data_api_credentials_configured

    data_err: str | None = None
    if leandna_data_api_credentials_configured():
        points, err = _fetch_datapoints_data_api(metric)
        if err is None:
            return points, None
        data_err = str(err.get("error") or err)

    if leandna_app_session_configured():
        points, err = _fetch_datapoints_app_api(metric)
        if err is None:
            return points, None
        return [], str(err.get("error") or err)

    return [], data_err or "no datapoint source available"


def _print_value_charts(rows: list[dict[str, Any]]) -> None:
    print()
    print(f"Last {_VALUES_POINT_COUNT} datapoints per metric:")
    for metric in rows:
        points, err = _fetch_last_datapoints(metric)
        if err and not points:
            mid = _metric_id(metric)
            name = metric_view_label(metric)
            print(f"\n=== {name} (id={mid}) ===")
            print(f"(datapoints unavailable: {err})")
            continue
        print_metric_value_chart(metric, points, max_points=_VALUES_POINT_COUNT)


def _fetch_via_data_api() -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    from src.leandna_data_api_http import leandna_data_api_credentials_configured
    from src.leandna_data_api_request import data_api_get_json
    from src.leandna_metrics_client import list_metric_definitions

    if not leandna_data_api_credentials_configured():
        raise RuntimeError(
            "Data API credentials not configured — set PR_LEANDNA_DATA_API_BEARER_TOKEN "
            "(Production) or ST_* (Staging), or refresh app session via bin/test-script."
        )

    env = data_api_get_json("identity", timeout_seconds=_READ_TIMEOUT_S)
    if not env.get("ok"):
        raise RuntimeError(f"GET /data/identity failed: {env.get('error') or env}")
    body = env.get("body")
    if not isinstance(body, dict):
        raise RuntimeError("GET /data/identity returned unexpected body")
    user_id = str(body.get("userId") or "").strip()
    if not user_id:
        raise RuntimeError("GET /data/identity did not return userId")
    label = str(body.get("userName") or body.get("emailAddress") or user_id).strip()
    catalog = list_metric_definitions(timeout_seconds=_READ_TIMEOUT_S)
    rows = [m for m in catalog if str(m.get("ownerId") or "").strip() == user_id]
    return rows, label, body


def _fetch_metrics(
    *,
    owner: str,
    kind: str,
) -> tuple[list[dict[str, Any]], str, dict[str, Any] | None, str]:
    """Return ``(rows, source, identity_or_none, owner_label)``."""
    identity: dict[str, Any] | None = None
    app_err: str | None = None
    if leandna_app_session_configured():
        try:
            rows = list_my_metrics_view(
                owner,
                owner_kind=kind,
                factory_ndx=LEANDNA_APP_FACTORY_NDX,
                timeout=_READ_TIMEOUT_S,
            )
            return rows, "app", identity, owner
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status != 401:
                raise
            app_err = str(e)
        except Exception as e:
            msg = str(e).lower()
            if "401" not in msg and "rejected" not in msg:
                raise
            app_err = str(e)

    try:
        rows, label, identity = _fetch_via_data_api()
        return rows, "data_api", identity, label
    except Exception as data_exc:
        if app_err:
            raise RuntimeError(
                "LeanDNA authentication failed on both paths.\n"
                f"  App session (Metrics/View): {app_err}\n"
                f"  Data API fallback (/data/identity): {data_exc}\n"
                "Refresh credentials in .env:\n"
                "  • App session: bin/test-script --show-session → set LEANDNA_APP_SESSION_ID\n"
                "  • Data API: copy a fresh PR_LEANDNA_DATA_API_BEARER_TOKEN (or "
                "PR_LEANDNA_DATA_API_COOKIE) from DevTools while logged into app.leandna.com"
            ) from data_exc
        raise


def main() -> int:
    ap = argparse.ArgumentParser(description="List metrics owned by you (.env configuration).")
    ap.add_argument(
        "--values",
        action="store_true",
        help=f"Show ASCII chart of last {_VALUES_POINT_COUNT} date/value pairs per metric",
    )
    ns = ap.parse_args()

    owner, identity, kind = resolve_app_metric_owner(
        factory_ndx=LEANDNA_APP_FACTORY_NDX,
        timeout=_READ_TIMEOUT_S,
    )

    if not owner and not leandna_app_session_configured():
        owner = "data_api"

    if leandna_app_session_configured() and not owner:
        print(
            "Could not determine metric owner — set LEANDNA_APP_METRIC_OWNER in .env "
            '(your name as shown in the app, e.g. "Marc Schriftman") or run whoami.',
            file=sys.stderr,
        )
        return 1

    if kind == "ndx" and LEANDNA_APP_USER_NDX and str(owner) == LEANDNA_APP_USER_NDX:
        print(
            f"Warning: using numeric LEANDNA_APP_USER_NDX={owner!r}. "
            "App Metrics/View usually expects metricOwner as your display name. "
            "Run whoami or set LEANDNA_APP_METRIC_OWNER.",
            file=sys.stderr,
        )

    try:
        rows, source, identity_from_fetch, owner_label = _fetch_metrics(
            owner=owner or "",
            kind=kind,
        )
    except Exception as e:
        print(f"Failed to fetch metrics: {e}", file=sys.stderr)
        return 1

    if identity is None:
        identity = identity_from_fetch

    rows = _sort_rows(rows)
    print(_auth_banner(source=source, owner_label=owner_label, owner=owner or "", kind=kind))

    payload = _build_payload(
        source=source,
        owner_label=owner_label,
        rows=rows,
        owner=owner or "",
        kind=kind,
        identity=identity,
    )
    print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
    if ns.values:
        _print_value_charts(rows)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
