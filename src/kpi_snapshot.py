"""Generate registry KPIs and persist them to the local SQLite store (S3-backed).

Reads are live generators. Writes go to ``kpi_observation`` (daily or month-close
grain). ``--dry-run`` runs generators and prints the rows without S3.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Sequence

from src.config import logger
from src.kpi_observation import observation_from_generator_raw
from src.kpi_store import (
    GRAIN_DAILY,
    GRAIN_MONTH,
    connect,
    default_kpi_store_path,
    grain_for_generator,
    stored_kpi_from_observation,
    upsert_kpi,
)
from src.kpi_store_s3 import (
    KPIStoreS3Error,
    download_kpi_store,
    kpi_store_s3_uri,
    upload_kpi_store,
)
from src.metrics_registry import (
    entry_has_tag,
    has_metric_generator,
    load_metrics_registry,
    registry_metric_tags,
)
from src.metrics_upsert import (
    MetricUpsertContext,
    MetricUpsertError,
    invoke_metric_generator,
    metrics_upsert_context_from_namespace,
)

def period_key_for(grain: str, as_of: date) -> str:
    if grain == GRAIN_MONTH:
        first = as_of.replace(day=1)
        prev = first - timedelta(days=1)
        return f"{prev.year:04d}-{prev.month:02d}"
    return as_of.isoformat()


def iter_history_snapshot_plan(until: date, months: int) -> list[tuple[date, str]]:
    """Month-end daily points plus previous-calendar-month scorecard as-ofs.

    ``until`` is included as both a daily and a month-close as-of (so today's
    trailing window and the latest completed month are stored). Then each of
    the previous *months* completed months gets a last-day daily snapshot and
    a 1st-of-following-month scorecard snapshot.
    """
    if months < 1:
        raise ValueError(f"history months must be >= 1, got {months}")
    seen: set[tuple[date, str]] = set()
    out: list[tuple[date, str]] = []

    def add(as_of: date, grain: str) -> None:
        key = (as_of, grain)
        if key in seen:
            return
        seen.add(key)
        out.append(key)

    add(until, GRAIN_DAILY)
    add(until, GRAIN_MONTH)
    cursor = until.replace(day=1)
    for _ in range(int(months)):
        month_end = cursor - timedelta(days=1)
        add(month_end, GRAIN_DAILY)
        add(cursor, GRAIN_MONTH)
        cursor = month_end.replace(day=1)
    return out


def _parse_as_of(raw: str) -> date:
    return date.fromisoformat(str(raw).strip()[:10])


def _default_db_path() -> Path:
    return default_kpi_store_path()


def iter_snapshot_metrics(
    registry: dict[str, Any],
    *,
    metric_name_filter: str | None = None,
    tag: str | None = None,
    grain: str | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    metrics = registry.get("metrics")
    if not isinstance(metrics, dict):
        return []
    want = (metric_name_filter or "").strip().casefold()
    want_grain = (grain or "").strip().lower() or None
    out: list[tuple[str, dict[str, Any]]] = []
    for name, entry in metrics.items():
        if not isinstance(entry, dict) or not has_metric_generator(entry):
            continue
        if want and str(name).casefold() != want:
            continue
        if tag and not entry_has_tag(entry, tag):
            continue
        gen = str(entry.get("metric-generator") or "").strip()
        if want_grain and grain_for_generator(gen) != want_grain:
            continue
        out.append((str(name), entry))
    return out


def _is_historical_sla_gap(error: str | None) -> bool:
    """JSM completed-cycle fields are often missing on older resolved tickets."""
    if not error:
        return False
    text = str(error)
    return (
        "no completed Time to first response SLA cycles" in text
        or "no completed Time to resolution SLA cycles" in text
        or "no completed TTFR/TTR SLA cycles" in text
    )
    import boto3

    return boto3.client("s3")


def run_kpi_snapshot(
    ctx: MetricUpsertContext,
    *,
    dry_run: bool,
    tag: str | None = None,
    grain: str | None = None,
    db_path: Path | None = None,
    s3_client: Any | None = None,
    s3_uri: str | None = None,
    skip_s3: bool = False,
    registry: dict[str, Any] | None = None,
    invoke=invoke_metric_generator,
    history_months: int | None = None,
) -> dict[str, Any]:
    """Generate registry KPIs and optionally persist them.

    ``dry_run`` or ``skip_s3`` skips S3. Persist to *db_path* when not ``dry_run``.
    ``history_months`` rebuilds month-end trailing windows plus month-close
    scorecard rows (requires ``--tag`` or ``--metric``).
    """
    if history_months:
        if not (tag or (ctx.metric_name_filter or "").strip()):
            raise ValueError(
                "--history-months requires --tag or --metric so unrelated "
                "generators are not stamped onto historical period keys"
            )
        until = _parse_as_of(ctx.entry_date)
        plan = [
            (as_of, g)
            for as_of, g in iter_history_snapshot_plan(until, int(history_months))
            if grain is None or g == grain
        ]
    else:
        plan = [(_parse_as_of(ctx.entry_date), grain)]

    reg = registry if registry is not None else load_metrics_registry()
    persist = not dry_run
    uri = (s3_uri if s3_uri is not None else kpi_store_s3_uri()) or ""
    use_s3 = persist and not skip_s3
    path = db_path or _default_db_path()

    if use_s3:
        if not uri:
            raise KPIStoreS3Error(
                "CORTEX_KPI_STORE_S3_URI is unset (expected s3://bucket/kpi/observations.sqlite)"
            )
        client = s3_client if s3_client is not None else _s3_client()
        download_kpi_store(path, s3_client=client, uri=uri)

    conn = connect(path) if persist else None
    written = 0
    failed: list[str] = []
    rows: list[dict[str, Any]] = []
    considered = 0

    for as_of, grain_filter in plan:
        day_ctx = replace(ctx, entry_date=as_of.isoformat())
        targets = iter_snapshot_metrics(
            reg,
            metric_name_filter=day_ctx.metric_name_filter,
            tag=tag,
            grain=grain_filter,
        )
        considered += len(targets)
        for name, entry in targets:
            gen = str(entry.get("metric-generator") or "").strip()
            row_grain = grain_for_generator(gen)
            period_key = period_key_for(row_grain, as_of)
            try:
                raw = invoke(
                    gen,
                    registry=reg,
                    ctx=day_ctx,
                    kpi_store_path=path,
                    skip_s3=True if persist else skip_s3,
                )
                obs = observation_from_generator_raw(raw, metric_name=name, origin="live")
            except (MetricUpsertError, TypeError, ValueError) as exc:
                obs = observation_from_generator_raw(
                    {"error": str(exc)}, metric_name=name, origin="live"
                )
            except Exception as exc:  # noqa: BLE001 — persist/report, then fail the run
                obs = observation_from_generator_raw(
                    {"error": f"{type(exc).__name__}: {exc}"},
                    metric_name=name,
                    origin="live",
                )

            stored = stored_kpi_from_observation(
                metric_name=name,
                grain=row_grain,
                period_key=period_key,
                observation=obs,
                generator=gen,
                tags=registry_metric_tags(entry),
            )
            rec = {
                "metric": name,
                "grain": row_grain,
                "period_key": period_key,
                "generator": gen,
                "ok": obs.ok,
                "value": obs.display_value,
                "error": obs.error,
            }
            if obs.error:
                if history_months and _is_historical_sla_gap(obs.error):
                    logger.warning(
                        "kpi-snapshot %s grain=%s period=%s skipped (no JSM SLA cycles in window)",
                        name,
                        row_grain,
                        period_key,
                    )
                    rec["ok"] = False
                    rec["skipped"] = True
                    rows.append(rec)
                    continue
                failed.append(f"{name} {row_grain}/{period_key}: {obs.error}")
                logger.error("kpi-snapshot %s failed: %s", name, obs.error)
            else:
                logger.info(
                    "kpi-snapshot %s grain=%s period=%s value=%s",
                    name,
                    row_grain,
                    period_key,
                    obs.display_value,
                )
            if persist and conn is not None:
                upsert_kpi(conn, stored)
                written += 1
            rows.append(rec)

    if conn is not None:
        conn.close()

    if use_s3 and persist:
        client = s3_client if s3_client is not None else _s3_client()
        upload_kpi_store(path, s3_client=client, uri=uri)

    summary = {
        "date": ctx.entry_date,
        "dry_run": dry_run,
        "history_months": history_months,
        "considered": considered,
        "written": written,
        "failed": failed,
        "ok": len(failed) == 0,
        "rows": rows,
    }
    return summary


def print_kpi_snapshot_summary(summary: dict[str, Any], *, as_json: bool = False) -> None:
    payload = {
        "date": summary.get("date"),
        "dry_run": summary.get("dry_run"),
        "considered": summary.get("considered"),
        "written": summary.get("written"),
        "ok": summary.get("ok"),
        "failed": summary.get("failed") or [],
    }
    if as_json:
        print(json.dumps(summary, default=str, indent=2), flush=True)
        return
    print(
        f"kpi-snapshot date={payload['date']} dry_run={payload['dry_run']} "
        f"considered={payload['considered']} written={payload['written']} "
        f"ok={payload['ok']}",
        flush=True,
    )
    for rec in summary.get("rows") or []:
        if rec.get("skipped"):
            print(
                f"  SKIP  {rec['metric']}  {rec['grain']}/{rec['period_key']}  {rec.get('error')}",
                flush=True,
            )
        elif rec.get("ok"):
            print(
                f"  OK  {rec['metric']}  {rec['grain']}/{rec['period_key']}  {rec.get('value')}",
                flush=True,
            )
        else:
            print(f"  FAIL  {rec['metric']}: {rec.get('error')}", flush=True)
    print("CORTEX_KPI_SNAPSHOT=" + json.dumps(payload, separators=(",", ":"), default=str), flush=True)


def kpi_snapshot_exit_code(summary: dict[str, Any]) -> int:
    return 0 if summary.get("ok") else 1


def add_kpi_snapshot_arguments(ap: argparse.ArgumentParser) -> None:
    from src.metrics_upsert import add_metrics_upsert_arguments

    add_metrics_upsert_arguments(ap)
    ap.add_argument("--tag", default=None, help="Only KPIs carrying this registry tag")
    ap.add_argument(
        "--grain",
        choices=("daily", "month"),
        default=None,
        help="Only daily snapshot KPIs or previous-calendar-month (scorecard) KPIs",
    )
    ap.add_argument(
        "--db",
        default=None,
        help="SQLite path (default: $CORTEX_CACHE_DIR/kpi/observations.sqlite)",
    )
    ap.add_argument(
        "--skip-s3",
        action="store_true",
        help="Write the local SQLite file only (no download/upload)",
    )
    ap.add_argument(
        "--history-months",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Rebuild N completed months of history (month-end trailing windows "
            "plus month-close KPIs). Requires --tag or --metric."
        ),
    )


def run_kpi_snapshot_cli(argv: Sequence[str] | None = None, *, prog: str = "kpi-snapshot") -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Generate my-metrics.yaml KPIs and store them in SQLite (S3-backed).",
    )
    add_kpi_snapshot_arguments(ap)
    ns = ap.parse_args(list(argv) if argv is not None else None)
    ctx = metrics_upsert_context_from_namespace(ns)
    db = Path(ns.db) if ns.db else None
    summary = run_kpi_snapshot(
        ctx,
        dry_run=bool(ns.dry_run),
        tag=ns.tag,
        grain=ns.grain,
        db_path=db,
        skip_s3=bool(ns.skip_s3) or bool(ns.dry_run),
        history_months=ns.history_months,
    )
    print_kpi_snapshot_summary(summary, as_json=ns.format == "json")
    return kpi_snapshot_exit_code(summary)


def main(argv: Sequence[str] | None = None) -> int:
    return run_kpi_snapshot_cli(argv, prog="kpi-snapshot")
