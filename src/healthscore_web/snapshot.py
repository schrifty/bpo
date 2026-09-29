"""Run Health Score generators into the separate Health Score SQLite store."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from typing import Any, Sequence

from src.healthscore_web.enhancement_engagement import (
    METRIC_NAME as ENHANCEMENT_ENGAGEMENT_METRIC,
    EnhancementEngagementGeneratorError,
    get_enhancement_engagement,
)
from src.healthscore_web.champion_login import (
    METRIC_NAME as CHAMPION_LOGIN_METRIC,
    ChampionLoginGeneratorError,
    get_champion_login_continuity,
)
from src.healthscore_web.champion_turnover import (
    METRIC_NAME as CHAMPION_TURNOVER_METRIC,
    ChampionTurnoverGeneratorError,
    get_champion_turnover,
)
from src.healthscore_web.roi_multiple import (
    METRIC_NAME as ROI_MULTIPLE_METRIC,
    RoiMultipleGeneratorError,
    get_roi_multiple,
)
from src.healthscore_web.call_sentiment import (
    METRIC_NAME as CALL_SENTIMENT_METRIC,
    CallSentimentGeneratorError,
    backfill_call_sentiment,
    get_call_sentiment,
)
from src.healthscore_web.call_talk_ratio import (
    METRIC_NAME as CALL_TALK_RATIO_METRIC,
    CallTalkRatioGeneratorError,
    backfill_call_talk_ratio,
    get_call_talk_ratio,
)
from src.healthscore_web.meeting_cadence import (
    METRIC_NAME as MEETING_CADENCE_METRIC,
    MeetingCadenceGeneratorError,
    get_meeting_cadence,
)
from src.healthscore_web.escalation_rate import (
    METRIC_NAME as ESCALATION_RATE_METRIC,
    EscalationRateGeneratorError,
    backfill_escalation_rate,
    get_escalation_rate,
)
from src.healthscore_web.sla_adherence import (
    METRIC_NAME as SLA_ADHERENCE_METRIC,
    SlaAdherenceGeneratorError,
    backfill_sla_adherence,
    get_sla_adherence,
)
from src.healthscore_web.ticket_volume_trend import (
    METRIC_NAME as TICKET_VOLUME_METRIC,
    TicketVolumeGeneratorError,
    backfill_ticket_volume_trend,
    get_ticket_volume_trend,
)
from src.healthscore_web.summit_attendance import (
    METRIC_NAME as SUMMIT_ATTENDANCE_METRIC,
    SummitAttendanceGeneratorError,
    get_summit_attendance,
)
from src.healthscore_web.usage_level import (
    METRIC_NAME as USAGE_LEVEL_METRIC,
    UsageLevelGeneratorError,
    get_usage_level,
)
from src.healthscore_web.store import connect, period_key_for, periods_for_metric
from src.healthscore_web.usage_trend import (
    GRAIN as USAGE_TREND_GRAIN,
    METRIC_NAME as USAGE_TREND_METRIC,
    UsageTrendGeneratorError,
    get_usage_trend,
)

SUPPORTED = (
    USAGE_LEVEL_METRIC,
    USAGE_TREND_METRIC,
    CHAMPION_LOGIN_METRIC,
    CHAMPION_TURNOVER_METRIC,
    ROI_MULTIPLE_METRIC,
    SUMMIT_ATTENDANCE_METRIC,
    ENHANCEMENT_ENGAGEMENT_METRIC,
    MEETING_CADENCE_METRIC,
    SLA_ADHERENCE_METRIC,
    TICKET_VOLUME_METRIC,
    ESCALATION_RATE_METRIC,
    CALL_SENTIMENT_METRIC,
    CALL_TALK_RATIO_METRIC,
    "all",
)


def _active_entities() -> list[dict[str, Any]]:
    from src.healthscore_web.api import _active_salesforce_entities

    try:
        return _active_salesforce_entities()
    except Exception as exc:  # noqa: BLE001
        raise UsageLevelGeneratorError(
            f"Salesforce Customer Entity inventory failed: {exc}"
        ) from exc


def run_usage_level_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    week_rows: list[dict[str, Any]] | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_usage_level(
        entities=entities,
        week_rows=week_rows,
        as_of=as_of,
        persist=not dry_run,
    )


def run_champion_login_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    sponsor_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_champion_login_continuity(
        entities=entities,
        sponsor_rows=sponsor_rows,
        as_of=as_of,
        persist=not dry_run,
    )


def run_champion_turnover_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    sponsor_rows: list[dict[str, Any]] | None = None,
    contacts: list[dict[str, Any]] | None = None,
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_champion_turnover(
        entities=entities,
        sponsor_rows=sponsor_rows,
        contacts=contacts,
        history=history,
        as_of=as_of,
        persist=not dry_run,
    )


def run_usage_trend_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_usage_trend(
        entities=entities,
        as_of=as_of,
        persist=not dry_run,
    )


def _stored_periods(metric_name: str) -> set[str]:
    conn = connect()
    try:
        return set(periods_for_metric(conn, metric_name))
    finally:
        conn.close()


def _load_usage_level_weeks(
    snapshots: list[dict[str, Any]],
    *,
    entities: list[dict[str, Any]],
    dry_run: bool,
    skip_periods: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Score usage_level from each workbook, oldest week first.

    Returns the per-week results and the ISO weeks skipped because a reading
    for that period was already in the store.
    """
    from src.cs_report_client import load_csr_report_by_file_id

    loaded: list[dict[str, Any]] = []
    skipped: list[str] = []
    for snap in sorted(snapshots, key=lambda row: str(row["period_key"])):
        key = str(snap["period_key"])
        if key in skip_periods:
            skipped.append(key)
            continue
        rows = load_csr_report_by_file_id(str(snap["id"]), name=str(snap["name"]))
        result = get_usage_level(
            entities=entities,
            week_rows=rows,
            as_of=snap["as_of"],
            period_as_of=snap["as_of"],
            persist=not dry_run,
        )
        loaded.append(
            {
                "period_key": key,
                "as_of": snap["as_of"].isoformat(),
                "file": snap["name"],
                "scored": result.get("scored"),
                "unmatched": result.get("unmatched"),
                "ok": result.get("ok"),
            }
        )
    return loaded, skipped


def backfill_usage_level_history(
    *,
    weeks: int,
    dry_run: bool = False,
    entities: list[dict[str, Any]] | None = None,
    only_missing: bool = False,
) -> dict[str, Any]:
    """Persist usage_level from the latest CS Report workbook in each ISO week.

    ``only_missing`` skips ISO weeks that already hold a usage_level reading, so a
    rerun only downloads workbooks for weeks the store has not seen.
    """
    from src.cs_report_client import (
        latest_csr_file_per_iso_week,
        list_csr_report_files,
    )

    if weeks < 1:
        raise UsageLevelGeneratorError("history weeks must be at least 1")
    if entities is None:
        entities = _active_entities()
    files = list_csr_report_files()
    snapshots = latest_csr_file_per_iso_week(files, weeks=weeks)
    if not snapshots:
        raise UsageLevelGeneratorError(
            "no CS Report workbooks found to backfill usage_level"
        )
    if len(snapshots) < weeks:
        raise UsageLevelGeneratorError(
            f"CS Report folder has {len(snapshots)} ISO weeks with workbooks, "
            f"need {weeks} for usage_level history"
        )
    skip = _stored_periods(USAGE_LEVEL_METRIC) if only_missing else set()
    loaded, skipped = _load_usage_level_weeks(
        snapshots, entities=entities, dry_run=dry_run, skip_periods=skip
    )
    return {
        "ok": True,
        "generator": "get_usage_level",
        "weeks_requested": weeks,
        "weeks_loaded": len(loaded),
        "weeks_skipped": skipped,
        "dry_run": dry_run,
        "weeks": loaded,
    }


def backfill_usage_trend_history(
    *,
    weeks: int,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    only_missing: bool = True,
) -> dict[str, Any]:
    """Persist usage_trend for each of the newest ``weeks`` ISO weeks.

    Each trend week reads the trailing ``WINDOW_WEEKS`` of stored usage_level,
    so the source history is first extended back ``weeks + WINDOW_WEEKS - 1``
    ISO weeks from whatever CS Report workbooks Drive still holds (weeks already
    in the store are not downloaded again). Drive running out earlier than that
    is a warning, not an error: those early trend weeks score on a shorter
    baseline and ``meta.weeks_used`` records how many weeks fed each reading.

    With ``only_missing`` (default) a trend week that already has readings is
    left alone; pass ``False`` to recompute every week in range.
    """
    from src.cs_report_client import (
        latest_csr_file_per_iso_week,
        list_csr_report_files,
    )
    from src.healthscore_web.usage_trend import WINDOW_WEEKS

    if weeks < 1:
        raise UsageTrendGeneratorError("history weeks must be at least 1")
    if entities is None:
        entities = _active_entities()
    today = as_of or date.today()
    source_weeks = weeks + WINDOW_WEEKS - 1
    files = list_csr_report_files()
    snapshots = latest_csr_file_per_iso_week(files, weeks=source_weeks)
    if not snapshots:
        raise UsageTrendGeneratorError(
            "no CS Report workbooks found to backfill usage_level for usage_trend"
        )
    warnings: list[str] = []
    if len(snapshots) < source_weeks:
        warnings.append(
            f"CS Report folder has {len(snapshots)} ISO weeks with workbooks; "
            f"{source_weeks} would give every trend week a full "
            f"{WINDOW_WEEKS}-week baseline. Older trend weeks use a shorter one."
        )
    source_loaded, source_skipped = _load_usage_level_weeks(
        snapshots,
        entities=entities,
        dry_run=dry_run,
        skip_periods=_stored_periods(USAGE_LEVEL_METRIC),
    )
    snapshot_by_key = {str(row["period_key"]): row for row in snapshots}
    existing_trend = _stored_periods(USAGE_TREND_METRIC) if only_missing else set()
    scored: list[dict[str, Any]] = []
    skipped: list[str] = []
    for offset in range(weeks - 1, -1, -1):
        day = today - timedelta(weeks=offset)
        key = period_key_for(USAGE_TREND_GRAIN, day)
        if key in existing_trend:
            skipped.append(key)
            continue
        snap = snapshot_by_key.get(key)
        week_as_of = snap["as_of"] if snap else day
        result = get_usage_trend(entities=entities, as_of=week_as_of, persist=not dry_run)
        scored.append(
            {
                "period_key": key,
                "as_of": week_as_of.isoformat(),
                "scored": result.get("scored"),
                "insufficient": result.get("insufficient"),
                "ok": result.get("ok"),
            }
        )
    return {
        "ok": True,
        "generator": "get_usage_trend",
        "metric_name": USAGE_TREND_METRIC,
        "weeks_requested": weeks,
        "weeks_scored": len(scored),
        "weeks_skipped": skipped,
        "source_weeks_requested": source_weeks,
        "source_weeks_available": len(snapshots),
        "source_weeks_loaded": [row["period_key"] for row in source_loaded],
        "source_weeks_skipped": source_skipped,
        "dry_run": dry_run,
        "warnings": warnings,
        "weeks": scored,
    }


def run_roi_multiple_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    week_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_roi_multiple(
        entities=entities,
        week_rows=week_rows,
        as_of=as_of,
        persist=not dry_run,
    )


def run_summit_attendance_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    campaigns: list[dict[str, Any]] | None = None,
    members: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_summit_attendance(
        entities=entities,
        campaigns=campaigns,
        members=members,
        as_of=as_of,
        persist=not dry_run,
    )


def run_enhancement_engagement_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    ideas: list[dict[str, Any]] | None = None,
    organizations: list[dict[str, Any]] | None = None,
    parent_ids: dict[str, str] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_enhancement_engagement(
        entities=entities,
        ideas=ideas,
        organizations=organizations,
        parent_ids=parent_ids,
        as_of=as_of,
        persist=not dry_run,
    )


def run_meeting_cadence_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    engagements: list[dict[str, Any]] | None = None,
    users: list[dict[str, Any]] | None = None,
    parent_ids: dict[str, str] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_meeting_cadence(
        entities=entities,
        engagements=engagements,
        users=users,
        parent_ids=parent_ids,
        as_of=as_of,
        persist=not dry_run,
    )


def run_call_sentiment_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    engagements: list[dict[str, Any]] | None = None,
    summaries: dict[str, str] | None = None,
    parent_ids: dict[str, str] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_call_sentiment(
        entities=entities,
        engagements=engagements,
        summaries=summaries,
        parent_ids=parent_ids,
        as_of=as_of,
        persist=not dry_run,
    )


def run_call_talk_ratio_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    engagements: list[dict[str, Any]] | None = None,
    shares: dict[str, Any] | None = None,
    parent_ids: dict[str, str] | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_call_talk_ratio(
        entities=entities,
        engagements=engagements,
        shares=shares,
        parent_ids=parent_ids,
        as_of=as_of,
        persist=not dry_run,
    )


def backfill_call_talk_ratio_history(
    *,
    months: int,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    only_missing: bool = True,
) -> dict[str, Any]:
    """Persist call_talk_ratio for each of the newest ``months`` calendar months."""
    if entities is None:
        entities = _active_entities()
    return backfill_call_talk_ratio(
        months=months,
        entities=entities,
        as_of=as_of,
        persist=not dry_run,
        only_missing=only_missing,
    )


def backfill_call_sentiment_history(
    *,
    months: int,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    only_missing: bool = True,
) -> dict[str, Any]:
    """Persist call_sentiment for each of the newest ``months`` calendar months."""
    if entities is None:
        entities = _active_entities()
    return backfill_call_sentiment(
        months=months,
        entities=entities,
        as_of=as_of,
        persist=not dry_run,
        only_missing=only_missing,
    )


def backfill_sla_adherence_history(
    *,
    months: int,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    only_missing: bool = True,
) -> dict[str, Any]:
    """Persist sla_adherence for each of the newest ``months`` snapshot dates."""
    if entities is None:
        entities = _active_entities()
    return backfill_sla_adherence(
        months=months,
        entities=entities,
        as_of=as_of,
        persist=not dry_run,
        only_missing=only_missing,
    )


def run_ticket_volume_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    issues: list[dict[str, Any]] | None = None,
    organizations_by_entity: dict[str, list[str]] | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_ticket_volume_trend(
        entities=entities,
        issues=issues,
        organizations_by_entity=organizations_by_entity,
        client=client,
        as_of=as_of,
        persist=not dry_run,
    )


def backfill_ticket_volume_history(
    *,
    months: int,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    only_missing: bool = True,
) -> dict[str, Any]:
    """Persist ticket_volume_trend for each of the newest ``months`` snapshot dates."""
    if entities is None:
        entities = _active_entities()
    return backfill_ticket_volume_trend(
        months=months,
        entities=entities,
        as_of=as_of,
        persist=not dry_run,
        only_missing=only_missing,
    )


def run_escalation_rate_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    issues: list[dict[str, Any]] | None = None,
    organizations_by_entity: dict[str, list[str]] | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_escalation_rate(
        entities=entities,
        issues=issues,
        organizations_by_entity=organizations_by_entity,
        client=client,
        as_of=as_of,
        persist=not dry_run,
    )


def backfill_escalation_rate_history(
    *,
    months: int,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    only_missing: bool = True,
) -> dict[str, Any]:
    """Persist escalation_rate for each of the newest ``months`` calendar months."""
    if entities is None:
        entities = _active_entities()
    return backfill_escalation_rate(
        months=months,
        entities=entities,
        as_of=as_of,
        persist=not dry_run,
        only_missing=only_missing,
    )


def run_sla_adherence_snapshot(
    *,
    dry_run: bool = False,
    as_of: date | None = None,
    entities: list[dict[str, Any]] | None = None,
    issues: list[dict[str, Any]] | None = None,
    organizations_by_entity: dict[str, list[str]] | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    if entities is None:
        entities = _active_entities()
    return get_sla_adherence(
        entities=entities,
        issues=issues,
        organizations_by_entity=organizations_by_entity,
        client=client,
        as_of=as_of,
        persist=not dry_run,
    )


def run_healthscore_snapshot(
    *,
    component: str = "all",
    dry_run: bool = False,
    as_of: date | None = None,
) -> dict[str, Any]:
    name = str(component or "all").strip().lower()
    if name not in SUPPORTED:
        raise ValueError(
            f"unknown Health Score component {component!r} (supported: {', '.join(SUPPORTED)})"
        )
    entities = _active_entities()
    results: dict[str, Any] = {"ok": True, "component": name}
    if name in (USAGE_LEVEL_METRIC, "all"):
        results[USAGE_LEVEL_METRIC] = run_usage_level_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (USAGE_TREND_METRIC, "all"):
        results[USAGE_TREND_METRIC] = run_usage_trend_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (CHAMPION_LOGIN_METRIC, "all"):
        results[CHAMPION_LOGIN_METRIC] = run_champion_login_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (CHAMPION_TURNOVER_METRIC, "all"):
        results[CHAMPION_TURNOVER_METRIC] = run_champion_turnover_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (ROI_MULTIPLE_METRIC, "all"):
        results[ROI_MULTIPLE_METRIC] = run_roi_multiple_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (SUMMIT_ATTENDANCE_METRIC, "all"):
        results[SUMMIT_ATTENDANCE_METRIC] = run_summit_attendance_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (ENHANCEMENT_ENGAGEMENT_METRIC, "all"):
        results[ENHANCEMENT_ENGAGEMENT_METRIC] = run_enhancement_engagement_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (MEETING_CADENCE_METRIC, "all"):
        results[MEETING_CADENCE_METRIC] = run_meeting_cadence_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (SLA_ADHERENCE_METRIC, "all"):
        results[SLA_ADHERENCE_METRIC] = run_sla_adherence_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (TICKET_VOLUME_METRIC, "all"):
        results[TICKET_VOLUME_METRIC] = run_ticket_volume_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (ESCALATION_RATE_METRIC, "all"):
        results[ESCALATION_RATE_METRIC] = run_escalation_rate_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (CALL_SENTIMENT_METRIC, "all"):
        results[CALL_SENTIMENT_METRIC] = run_call_sentiment_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    if name in (CALL_TALK_RATIO_METRIC, "all"):
        results[CALL_TALK_RATIO_METRIC] = run_call_talk_ratio_snapshot(
            dry_run=dry_run, as_of=as_of, entities=entities
        )
    return results


def run_healthscore_snapshot_cli(
    argv: Sequence[str] | None = None,
    *,
    prog: str = "cortex healthscore-snapshot",
) -> int:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Run Health Score generators into the Health Score store. "
            "Does not write KPI observations. Default runs usage_level, then "
            "usage_trend, champion_login_continuity, champion_turnover, roi_multiple, "
            "summit_attendance, "
            "enhancement_engagement, meeting_cadence, sla_adherence, "
            "ticket_volume_trend, escalation_rate, call_sentiment, then call_talk_ratio."
        ),
    )
    parser.add_argument(
        "--component",
        default="all",
        help=(
            "usage_level, usage_trend, champion_login_continuity, champion_turnover, "
            "roi_multiple, "
            "summit_attendance, enhancement_engagement, meeting_cadence, "
            "sla_adherence, ticket_volume_trend, escalation_rate, call_sentiment, "
            "call_talk_ratio, or all"
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", dest="as_of", default=None, help="YYYY-MM-DD fallback period")
    parser.add_argument(
        "--history-weeks",
        type=int,
        default=None,
        help=(
            "Backfill usage_level from one CS Report workbook per ISO week (newest "
            "weeks). With --component usage_trend or all, also score usage_trend "
            "for each of those weeks."
        ),
    )
    parser.add_argument(
        "--history-months",
        type=int,
        default=None,
        help=(
            "Backfill the newest N months for call_sentiment, call_talk_ratio, "
            "sla_adherence, ticket_volume_trend, or escalation_rate"
        ),
    )
    parser.add_argument(
        "--refresh-history",
        action="store_true",
        help=(
            "With --history-weeks or --history-months, recompute periods that "
            "already have readings instead of skipping them"
        ),
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    try:
        if args.history_months and args.history_weeks:
            raise ValueError("pass only one of --history-months and --history-weeks")
        if args.history_months:
            name = str(args.component or "all").strip().lower()
            history_kwargs = {
                "months": args.history_months,
                "dry_run": args.dry_run,
                "as_of": as_of,
                "only_missing": not args.refresh_history,
            }
            history_runners = {
                CALL_SENTIMENT_METRIC: backfill_call_sentiment_history,
                CALL_TALK_RATIO_METRIC: backfill_call_talk_ratio_history,
                SLA_ADHERENCE_METRIC: backfill_sla_adherence_history,
                TICKET_VOLUME_METRIC: backfill_ticket_volume_history,
                ESCALATION_RATE_METRIC: backfill_escalation_rate_history,
            }
            if name == "all":
                result = {
                    key: runner(**history_kwargs) for key, runner in history_runners.items()
                }
                result["ok"] = True
                result["component"] = "all"
            elif name in history_runners:
                result = history_runners[name](**history_kwargs)
            else:
                raise ValueError(
                    "--history-months applies to call_sentiment, call_talk_ratio, "
                    "sla_adherence, ticket_volume_trend, or escalation_rate"
                )
        elif args.history_weeks:
            only_missing = not args.refresh_history
            result: dict[str, Any] = backfill_usage_level_history(
                weeks=args.history_weeks,
                dry_run=args.dry_run,
                only_missing=only_missing,
            )
            name = str(args.component or "all").strip().lower()
            if name in (USAGE_TREND_METRIC, "all"):
                result[USAGE_TREND_METRIC] = backfill_usage_trend_history(
                    weeks=args.history_weeks,
                    dry_run=args.dry_run,
                    as_of=as_of,
                    only_missing=only_missing,
                )
        else:
            result = run_healthscore_snapshot(
                component=args.component, dry_run=args.dry_run, as_of=as_of
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (
        UsageLevelGeneratorError,
        UsageTrendGeneratorError,
        ChampionLoginGeneratorError,
        ChampionTurnoverGeneratorError,
        RoiMultipleGeneratorError,
        SummitAttendanceGeneratorError,
        EnhancementEngagementGeneratorError,
        MeetingCadenceGeneratorError,
        SlaAdherenceGeneratorError,
        TicketVolumeGeneratorError,
        EscalationRateGeneratorError,
        CallSentimentGeneratorError,
        CallTalkRatioGeneratorError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, default=str, indent=2))
    return 0
