"""Run metric generators from ``config/my-metrics.yaml`` and upsert LeanDNA datapoints."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any, Callable, Sequence

from src.leandna_metric_registry_resolve import (
    MetricRegistryResolveError,
    resolve_registry_metric_id,
)
from src.leandna_metrics_write import MetricWriteArgs, run_upsert
from src.metrics_registry import (
    get_kpi_automation_pct,
    is_upsertable_metric,
    load_metrics_registry,
    metric_registry_skip_reason,
)


class MetricUpsertError(Exception):
    """One metric failed generation or write."""


@dataclass(frozen=True)
class MetricUpsertContext:
    entry_date: str
    requested_sites: str | None
    skip_catalog: bool
    timeout_seconds: float
    verbose: bool
    dry_run: bool
    days: int
    max_issues_per_board: int
    workers: int
    metric_name_filter: str | None


@dataclass(frozen=True)
class MetricParts:
    numerator: float
    denominator: float


def count_metrics_with_id(registry: dict[str, Any]) -> int:
    metrics = registry.get("metrics")
    if not isinstance(metrics, dict):
        return 0
    n = 0
    for entry in metrics.values():
        if isinstance(entry, dict) and entry.get("metric-id") is not None:
            n += 1
    return n


def parse_generator_parts(
    raw: Any,
    *,
    metric_name: str,
    registry: dict[str, Any],
) -> MetricParts:
    """Normalize generator return values to numerator/denominator for Data API POST."""
    if raw is None:
        raise MetricUpsertError("generator returned None")

    if isinstance(raw, MetricParts):
        return raw

    if isinstance(raw, dict):
        err = raw.get("error")
        if err:
            raise MetricUpsertError(str(err))
        if "numerator" in raw and "denominator" in raw:
            return MetricParts(float(raw["numerator"]), float(raw["denominator"]))
        if "numerator" in raw:
            denom = raw.get("denominator", 1)
            return MetricParts(float(raw["numerator"]), float(denom))
        if "value" in raw and raw.get("value") is not None:
            return MetricParts(float(raw["value"]), 1.0)
        if "teams" in raw or raw.get("mode") == "history":
            return _parts_from_cycle_time_payload(raw)
        raise MetricUpsertError(f"unsupported dict result keys: {sorted(raw.keys())[:8]}")

    if isinstance(raw, (int, float)):
        if metric_name.rstrip().endswith("%"):
            total = count_metrics_with_id(registry)
            if total <= 0:
                raise MetricUpsertError("no metrics with metric-id in registry for % denominator")
            return MetricParts(float(raw), float(total))
        return MetricParts(float(raw), 1.0)

    raise MetricUpsertError(f"unsupported generator result type: {type(raw).__name__}")


def _parts_from_cycle_time_payload(payload: dict[str, Any]) -> MetricParts:
    medians: list[float] = []
    for team in payload.get("teams") or []:
        if not isinstance(team, dict) or team.get("error"):
            continue
        raw_med = team.get("median_days")
        if raw_med is None:
            overall = team.get("overall") or {}
            raw_med = overall.get("median_days")
        if raw_med is not None:
            medians.append(float(raw_med))
    if not medians:
        raise MetricUpsertError("no median_days from development cycle time teams")
    return MetricParts(median(medians), 1.0)


def _invoke_get_dev_team_cycle_times(ctx: dict[str, Any]) -> dict[str, Any]:
    from src.jira_client import get_shared_jira_client
    from src.jira_cycle_time import get_dev_team_cycle_times

    jira = get_shared_jira_client()
    return get_dev_team_cycle_times(
        jira,
        days=int(ctx.get("days") or 30),
        months=None,
        max_issues_per_board=int(ctx.get("max_issues_per_board") or 500),
        workers=int(ctx.get("workers") or 6),
        timeout=float(ctx.get("timeout") or 60.0),
    )


def _invoke_get_dev_team_lead_time(ctx: dict[str, Any]) -> dict[str, Any]:
    from src.jira_client import get_shared_jira_client
    from src.jira_cycle_time import get_dev_team_lead_time_metric_value

    jira = get_shared_jira_client()
    return get_dev_team_lead_time_metric_value(
        jira,
        days=int(ctx.get("days") or 30),
        max_issues_per_board=int(ctx.get("max_issues_per_board") or 500),
        workers=int(ctx.get("workers") or 6),
        timeout=float(ctx.get("timeout") or 60.0),
    )


def _invoke_get_ai_token_usage(ctx: dict[str, Any]) -> dict[str, Any]:
    from src.cursor_ai_usage_metrics import get_ai_token_usage_value
    from src.cursor_client import get_shared_cursor_client

    client = get_shared_cursor_client()
    return get_ai_token_usage_value(
        client,
        days=int(ctx.get("days") or 30),
        timeout=float(ctx.get("timeout") or 60.0),
    )


def _invoke_get_service_threshold_tickets(ctx: dict[str, Any]) -> dict[str, Any]:
    from src.jira_client import get_shared_jira_client
    from src.jira_service_threshold_tickets import get_service_threshold_ticket_count

    jira = get_shared_jira_client()
    return get_service_threshold_ticket_count(
        jira,
        timeout=float(ctx.get("timeout") or 60.0),
    )


def _invoke_get_sprint_delivery_by_team(ctx: dict[str, Any]) -> dict[str, Any]:
    from src.jira_client import get_shared_jira_client
    from src.jira_sprint_delivery import get_sprint_delivery_metric_value

    jira = get_shared_jira_client()
    return get_sprint_delivery_metric_value(
        jira,
        max_issues_per_board=int(ctx.get("max_issues_per_board") or 500),
        timeout=float(ctx.get("timeout") or 60.0),
    )


def _invoke_get_sprint_story_points_by_team(ctx: dict[str, Any]) -> dict[str, Any]:
    from src.jira_client import get_shared_jira_client
    from src.jira_sprint_story_points import get_sprint_story_points_metric_value

    jira = get_shared_jira_client()
    return get_sprint_story_points_metric_value(
        jira,
        max_issues_per_board=int(ctx.get("max_issues_per_board") or 500),
        timeout=float(ctx.get("timeout") or 60.0),
    )


_GENERATORS: dict[str, Callable[[dict[str, Any]], Any]] = {
    "get_kpi_automation_pct": lambda ctx: get_kpi_automation_pct(registry=ctx["registry"]),
    "get_dev_team_cycle_times": _invoke_get_dev_team_cycle_times,
    "get_dev_team_lead_time": _invoke_get_dev_team_lead_time,
    "get_ai_token_usage": _invoke_get_ai_token_usage,
    "get_service_threshold_tickets": _invoke_get_service_threshold_tickets,
    "get_sprint_delivery_by_team": _invoke_get_sprint_delivery_by_team,
    "get_sprint_story_points_by_team": _invoke_get_sprint_story_points_by_team,
}


def invoke_metric_generator(name: str, *, registry: dict[str, Any], ctx: MetricUpsertContext) -> Any:
    """Call a registry ``metric-generator`` by name."""
    fn = _GENERATORS.get(name)
    if fn is None:
        raise MetricUpsertError(f"unknown metric-generator {name!r} (not registered in metrics_upsert)")
    call_ctx = {
        "registry": registry,
        "days": ctx.days,
        "max_issues_per_board": ctx.max_issues_per_board,
        "workers": ctx.workers,
        "timeout": ctx.timeout_seconds,
    }
    return fn(call_ctx)


def iter_metrics_to_upsert(
    registry: dict[str, Any],
    *,
    metric_name_filter: str | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    """Registry rows with a non-empty ``metric-generator``, optionally filtered by name."""
    metrics = registry.get("metrics")
    if not isinstance(metrics, dict):
        return []
    out: list[tuple[str, dict[str, Any]]] = []
    want = (metric_name_filter or "").strip().casefold()
    for name, entry in metrics.items():
        if not isinstance(entry, dict):
            continue
        if not is_upsertable_metric(entry):
            continue
        if want and name.casefold() != want:
            continue
        out.append((name, entry))
    return out


def upsert_one_registry_metric(
    metric_name: str,
    entry: dict[str, Any],
    *,
    registry: dict[str, Any],
    ctx: MetricUpsertContext,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    """Generate value and upsert one registry metric."""
    gen_name = str(entry.get("metric-generator") or "").strip()

    try:
        resolution = resolve_registry_metric_id(
            metric_name,
            entry,
            requested_sites=ctx.requested_sites,
            dry_run=ctx.dry_run,
            timeout_seconds=ctx.timeout_seconds,
            registry_path=registry_path,
        )
    except MetricRegistryResolveError as e:
        raise MetricUpsertError(str(e)) from e

    metric_id = resolution.metric_id

    raw = invoke_metric_generator(gen_name, registry=registry, ctx=ctx)
    parts = parse_generator_parts(raw, metric_name=metric_name, registry=registry)

    row: dict[str, Any] = {
        "metric": metric_name,
        "metric_id": metric_id,
        "generator": gen_name,
        "date": ctx.entry_date,
        "numerator": parts.numerator,
        "denominator": parts.denominator,
        "metric_id_source": resolution.source,
    }
    if resolution.detail:
        row["metric_id_detail"] = resolution.detail

    if ctx.dry_run:
        row["ok"] = True
        row["dry_run"] = True
        return row

    write_args = MetricWriteArgs(
        metric_id=metric_id,
        entry_date=ctx.entry_date,
        numerator=parts.numerator,
        denominator=parts.denominator,
        requested_sites=ctx.requested_sites,
        category=None,
        skip_catalog=ctx.skip_catalog,
        timeout_seconds=ctx.timeout_seconds,
        verbose=ctx.verbose,
    )
    code, env = run_upsert(write_args)
    row["write"] = env
    row["ok"] = code == 0 and bool(env.get("ok"))
    if not row["ok"]:
        err = env.get("error") or (env.get("insert") or {}).get("error") or "upsert failed"
        row["error"] = err
    return row


def _registry_metric_entry(
    registry: dict[str, Any],
    metric_name: str,
) -> tuple[str, dict[str, Any]] | None:
    """Exact registry name match (case-sensitive key in YAML)."""
    metrics = registry.get("metrics")
    if not isinstance(metrics, dict):
        return None
    entry = metrics.get(metric_name)
    if isinstance(entry, dict):
        return metric_name, entry
    return None


def _collect_skipped_metrics(registry: dict[str, Any]) -> dict[str, list[str]]:
    """Group skipped registry names by reason."""
    buckets: dict[str, list[str]] = {
        "no_generator": [],
        "other": [],
    }
    metrics = registry.get("metrics")
    if not isinstance(metrics, dict):
        return buckets
    for name, entry in metrics.items():
        if not isinstance(entry, dict):
            buckets["other"].append(name)
            continue
        reason = metric_registry_skip_reason(entry)
        if reason is None:
            continue
        if reason == "no generator":
            buckets["no_generator"].append(name)
        else:
            buckets["other"].append(name)
    return buckets


def run_metrics_upsert(ctx: MetricUpsertContext, *, registry_path: Path | None = None) -> dict[str, Any]:
    """Run all configured generators and upsert datapoints for today's (or chosen) date."""
    registry = load_metrics_registry(path=registry_path)

    if ctx.metric_name_filter:
        want = ctx.metric_name_filter.strip()
        hit = _registry_metric_entry(registry, want)
        if hit is None:
            return {
                "ok": False,
                "error": f"metric {want!r} not found in config/my-metrics.yaml",
                "date": ctx.entry_date,
                "dry_run": ctx.dry_run,
                "attempted": 0,
                "results": [],
            }
        _name, entry = hit
        skip = metric_registry_skip_reason(entry)
        if skip:
            return {
                "ok": False,
                "error": f"metric {want!r} cannot upsert: {skip}",
                "date": ctx.entry_date,
                "dry_run": ctx.dry_run,
                "attempted": 0,
                "results": [],
            }

    candidates = iter_metrics_to_upsert(registry, metric_name_filter=ctx.metric_name_filter)
    skipped_buckets = _collect_skipped_metrics(registry)

    results: list[dict[str, Any]] = []
    failures: list[str] = []

    for name, entry in candidates:
        try:
            results.append(
                upsert_one_registry_metric(
                    name,
                    entry,
                    registry=registry,
                    ctx=ctx,
                    registry_path=registry_path,
                )
            )
        except (MetricUpsertError, ValueError) as e:
            failures.append(name)
            results.append(
                {
                    "metric": name,
                    "metric_id": entry.get("metric-id"),
                    "generator": entry.get("metric-generator"),
                    "ok": False,
                    "error": str(e),
                }
            )
        except Exception as e:
            failures.append(name)
            results.append(
                {
                    "metric": name,
                    "metric_id": entry.get("metric-id"),
                    "generator": entry.get("metric-generator"),
                    "ok": False,
                    "error": f"{type(e).__name__}: {e}",
                }
            )

    summary = {
        "ok": not failures,
        "date": ctx.entry_date,
        "dry_run": ctx.dry_run,
        "attempted": len(candidates),
        "skipped_no_generator": skipped_buckets["no_generator"],
        "skipped_other": skipped_buckets["other"],
        "results": results,
    }
    if failures:
        summary["failed"] = failures
    return summary


def print_metrics_upsert_summary(summary: dict[str, Any], *, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(summary, indent=2, default=str))
        return
    print(f"metrics-upsert date={summary.get('date')} dry_run={summary.get('dry_run')}")
    if summary.get("error"):
        print(f"Error: {summary['error']}", file=sys.stderr)
    skipped_gen = summary.get("skipped_no_generator") or []
    skipped_other = summary.get("skipped_other") or []
    if skipped_gen:
        print(f"Skipped (no generator): {', '.join(skipped_gen)}")
    if skipped_other:
        print(f"Skipped: {', '.join(skipped_other)}")
    for row in summary.get("results") or []:
        name = row.get("metric")
        if row.get("ok"):
            num = row.get("numerator")
            den = row.get("denominator")
            suffix = " (dry run)" if row.get("dry_run") else ""
            if den == 100 or den == 100.0:
                print(f"  OK   {name} id={row.get('metric_id')} {num}%{suffix}")
            else:
                print(f"  OK   {name} id={row.get('metric_id')} {num}/{den}{suffix}")
        else:
            print(f"  FAIL {name}: {row.get('error')}", file=sys.stderr)


def metrics_upsert_exit_code(summary: dict[str, Any]) -> int:
    if summary.get("error"):
        return 1
    return 0 if summary.get("ok") else 1


def add_metrics_upsert_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Datapoint date (default: today)",
    )
    ap.add_argument(
        "--metric",
        default=None,
        help="Upsert only this registry display name (exact match)",
    )
    ap.add_argument("--requested-sites", default=None, metavar="ID")
    ap.add_argument("--skip-catalog", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="Generate values only; no Data API write")
    ap.add_argument("--days", type=int, default=30, help="Trailing window for Jira cycle time (default: 30)")
    ap.add_argument("--max-issues", type=int, default=500, dest="max_issues")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--timeout", type=float, default=120.0, metavar="SEC")
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument("-v", "--verbose", action="store_true")


def metrics_upsert_context_from_namespace(ns: argparse.Namespace) -> MetricUpsertContext:
    return MetricUpsertContext(
        entry_date=ns.date,
        requested_sites=ns.requested_sites,
        skip_catalog=ns.skip_catalog,
        timeout_seconds=ns.timeout,
        verbose=ns.verbose,
        dry_run=ns.dry_run,
        days=ns.days,
        max_issues_per_board=ns.max_issues,
        workers=ns.workers,
        metric_name_filter=ns.metric,
    )


def run_metrics_upsert_cli(argv: Sequence[str] | None = None, *, prog: str = "metrics-upsert") -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Run my-metrics.yaml generators and upsert LeanDNA MetricDataPoint rows.",
    )
    add_metrics_upsert_arguments(ap)
    ns = ap.parse_args(list(argv) if argv is not None else None)
    ctx = metrics_upsert_context_from_namespace(ns)
    summary = run_metrics_upsert(ctx)
    print_metrics_upsert_summary(summary, as_json=ns.format == "json")
    return metrics_upsert_exit_code(summary)


def main(argv: Sequence[str] | None = None) -> int:
    return run_metrics_upsert_cli(argv, prog="metrics-upsert")
