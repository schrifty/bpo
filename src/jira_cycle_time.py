"""Development-team cycle time from Jira board issues (changelog-based)."""

from __future__ import annotations

import os
import logging
import statistics
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from .jira_client import JiraClient

logger = logging.getLogger("bpo")

# Boards selected for dev-team cycle time (see discover-dev-teams).
DEV_CYCLE_TIME_BOARDS: tuple[dict[str, Any], ...] = (
    {
        "board_id": 44,
        "name": "LEAN Scrum - CURRENT Issues and Planning",
        "project_key": "LEAN",
        "team_label": "LEAN Engineering",
    },
    {
        "board_id": 36,
        "name": "Active Scrum Board",
        "project_key": "CUSTOMER",
        "team_label": "CUSTOMER Active Scrum",
    },
    {
        "board_id": 46,
        "name": "Data Integration Sprint Board",
        "project_key": "CUSTOMER",
        "team_label": "Data Integration",
    },
    {
        "board_id": 322,
        "name": "Backlog by Date",
        "project_key": "CUSTOMER",
        "team_label": "CUSTOMER Backlog by Date",
    },
)

_AGILE = "/rest/agile/1.0"
_ACTIVE_CATEGORY = "indeterminate"

# Default issuetype exclusions (see ``get-dev-cycle-times``). SUT and Sub-task are kept
# (implementation / go-live work). Long tails are trimmed via σ outlier filter instead.
DEFAULT_EXCLUDED_ISSUE_TYPES: tuple[str, ...] = (
    "Epic",
    "Hypercare",
    "Data Sync Escalation",
    "Data Access",
    "Request for Information",
)


def parse_excluded_issue_types(
    extra: list[str] | tuple[str, ...] | None = None,
    *,
    include_all: bool = False,
) -> tuple[str, ...]:
    """Issue type names to drop from cycle-time cohorts."""
    if include_all:
        return ()
    if extra:
        return tuple(x.strip() for x in extra if x and str(x).strip())
    raw = (os.environ.get("BPO_CYCLE_TIME_EXCLUDE_ISSUE_TYPES") or "").strip()
    if raw:
        return tuple(x.strip() for x in raw.split(",") if x.strip())
    return DEFAULT_EXCLUDED_ISSUE_TYPES


def _jql_quote_type(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return '""'
    if " " in n or "-" in n:
        return f'"{n}"'
    return n


def build_done_issues_jql(
    days: int,
    excluded_issue_types: tuple[str, ...],
) -> str:
    jql = f"statusCategory = Done AND resolved >= -{int(days)}d"
    if excluded_issue_types:
        quoted = ", ".join(_jql_quote_type(t) for t in excluded_issue_types)
        jql += f" AND issuetype not in ({quoted})"
    return jql


def _issue_type_name(fields: dict[str, Any]) -> str:
    it = fields.get("issuetype")
    if isinstance(it, dict):
        return str(it.get("name") or "").strip()
    return ""


def _issue_excluded(
    fields: dict[str, Any],
    excluded_issue_types: tuple[str, ...],
) -> bool:
    name = _issue_type_name(fields)
    if not name:
        return False
    excluded_lower = {t.lower() for t in excluded_issue_types}
    return name.lower() in excluded_lower


@dataclass(frozen=True)
class BoardCycleTimeSummary:
    board_id: int
    team_label: str
    board_name: str
    project_key: str
    window_days: int
    completed_count: int
    measured_count: int
    skipped_no_active: int
    median_days: float | None
    mean_days: float | None
    p85_days: float | None
    min_days: float | None
    max_days: float | None


def _parse_jira_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Jira often uses +0000 without a colon (Python 3.9 fromisoformat needs +00:00).
    if len(text) >= 5 and text[-5] in "+-" and text[-3:].isdigit() and text[-6] not in ":":
        text = f"{text[:-5]}{text[-5:-2]}:{text[-2:]}"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_status_category_map(client: JiraClient, *, timeout: float = 45.0) -> dict[str, str]:
    """Map status name (lower case) → category key (``new`` / ``indeterminate`` / ``done``)."""
    resp = requests.get(
        f"{client.base_url}/rest/api/3/status",
        headers=client._headers,
        timeout=timeout,
    )
    resp.raise_for_status()
    out: dict[str, str] = {}
    for row in resp.json():
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip().lower()
        cat = row.get("statusCategory") or {}
        key = ""
        if isinstance(cat, dict):
            key = str(cat.get("key") or "").strip().lower()
        if name and key:
            out[name] = key
    return out


def _category_for_status(status_name: str | None, status_map: dict[str, str]) -> str:
    return status_map.get(str(status_name or "").strip().lower(), "new")


def active_cycle_days_from_changelog(
    histories: list[dict[str, Any]],
    *,
    status_map: dict[str, str],
    created: datetime | None,
    initial_status: str | None = None,
) -> float | None:
    """Sum calendar days in ``indeterminate`` (In Progress / In Review) before first ``done``.

    Returns ``None`` if the issue never entered an active (in-progress) status.
    """
    events: list[tuple[datetime, str]] = []
    if created is not None:
        events.append((created, str(initial_status or "Open")))

    for hist in sorted(histories, key=lambda h: str(h.get("created") or "")):
        when = _parse_jira_dt(hist.get("created"))
        if when is None:
            continue
        for item in hist.get("items") or []:
            if not isinstance(item, dict) or item.get("field") != "status":
                continue
            to_status = str(item.get("toString") or "").strip() or "Unknown"
            events.append((when, to_status))

    if not events:
        return None

    events.sort(key=lambda x: x[0])
    total_seconds = 0.0
    saw_active = False
    finished = False

    for i, (start, status_name) in enumerate(events):
        if finished:
            break
        end = events[i + 1][0] if i + 1 < len(events) else start
        if end <= start:
            continue
        cat = _category_for_status(status_name, status_map)
        if cat == _ACTIVE_CATEGORY:
            saw_active = True
            total_seconds += (end - start).total_seconds()
        elif cat == "done" and saw_active:
            finished = True

    if not saw_active:
        return None
    return total_seconds / 86400.0


def _fetch_issue_changelog(
    client: JiraClient,
    issue_key: str,
    *,
    timeout: float = 45.0,
) -> list[dict[str, Any]]:
    histories: list[dict[str, Any]] = []
    start_at = 0
    page_size = 100
    while True:
        resp = requests.get(
            f"{client.base_url}/rest/api/3/issue/{issue_key}/changelog",
            headers=client._headers,
            params={"startAt": start_at, "maxResults": page_size},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        chunk = data.get("values") or []
        histories.extend(x for x in chunk if isinstance(x, dict))
        if data.get("isLast") is True:
            break
        if not chunk:
            break
        start_at += len(chunk)
    return histories


def _fetch_board_done_issues(
    client: JiraClient,
    board_id: int,
    *,
    days: int,
    max_issues: int,
    excluded_issue_types: tuple[str, ...] = (),
    timeout: float = 60.0,
) -> tuple[list[dict[str, Any]], int | None, str]:
    """Issues on *board_id* completed within the trailing window (by ``resolved``)."""
    jql = build_done_issues_jql(days, excluded_issue_types)
    issues: list[dict[str, Any]] = []
    start_at = 0
    page_size = min(100, max(1, max_issues))
    reported_total: int | None = None

    while len(issues) < max_issues:
        resp = requests.get(
            f"{client.base_url}{_AGILE}/board/{board_id}/issue",
            headers=client._headers,
            params={
                "jql": jql,
                "startAt": start_at,
                "maxResults": min(page_size, max_issues - len(issues)),
                "fields": "summary,status,created,resolutiondate,issuetype",
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if reported_total is None and data.get("total") is not None:
            try:
                reported_total = int(data["total"])
            except (TypeError, ValueError):
                pass
        chunk = data.get("issues") or []
        for x in chunk:
            if not isinstance(x, dict):
                continue
            fields = x.get("fields") or {}
            if isinstance(fields, dict) and _issue_excluded(fields, excluded_issue_types):
                continue
            issues.append(x)
        if not chunk:
            break
        if data.get("isLast") is True:
            break
        if reported_total is not None and len(issues) >= reported_total:
            break
        start_at += len(chunk)

    return issues[:max_issues], reported_total, jql


def _resolution_month(fields: dict[str, Any]) -> str | None:
    resolved = _parse_jira_dt(fields.get("resolutiondate"))
    if resolved is None:
        return None
    return resolved.strftime("%Y-%m")


def trailing_month_periods(months: int, *, end: date | None = None) -> list[str]:
    """Last *months* calendar months including partial current month (``YYYY-MM``)."""
    n = max(1, int(months))
    today = end or date.today()
    y, m = today.year, today.month
    out: list[str] = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m < 1:
            m = 12
            y -= 1
    return list(reversed(out))


def history_window_days(months: int) -> int:
    """Trailing day count covering *months* full calendar months (inclusive buffer)."""
    n = max(1, int(months))
    end = date.today()
    y, m = end.year, end.month
    for _ in range(n - 1):
        m -= 1
        if m < 1:
            m = 12
            y -= 1
    start = date(y, m, 1)
    return (end - start).days + 1


def history_fetch_cap(months: int, max_issues_per_board: int) -> int:
    """Raise fetch cap for longer histories (~400 issues/month, floor 2000, ceiling 10000)."""
    scaled = max(2000, int(months) * 400)
    return min(10_000, max(int(max_issues_per_board), scaled))


@dataclass
class IssueCycleRow:
    key: str
    cycle_days: float | None
    resolved_month: str | None
    issue_type: str
    skipped_no_active: bool


def _issue_cycle_row(
    client: JiraClient,
    issue: dict[str, Any],
    *,
    status_map: dict[str, str],
    timeout: float,
) -> IssueCycleRow:
    key = str(issue.get("key") or "")
    fields = issue.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    created = _parse_jira_dt(fields.get("created"))
    initial = None
    status = fields.get("status")
    if isinstance(status, dict):
        initial = status.get("name")
    histories = _fetch_issue_changelog(client, key, timeout=timeout)
    days = active_cycle_days_from_changelog(
        histories,
        status_map=status_map,
        created=created,
        initial_status=initial,
    )
    return IssueCycleRow(
        key=key,
        cycle_days=days,
        resolved_month=_resolution_month(fields),
        issue_type=_issue_type_name(fields) or "Unknown",
        skipped_no_active=days is None,
    )


def _compute_issue_cycle_rows(
    client: JiraClient,
    issues: list[dict[str, Any]],
    *,
    status_map: dict[str, str],
    workers: int,
    timeout: float,
) -> list[IssueCycleRow]:
    rows: list[IssueCycleRow] = []
    if not issues:
        return rows
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 12))) as pool:
        futures = [
            pool.submit(
                _issue_cycle_row,
                client,
                issue,
                status_map=status_map,
                timeout=timeout,
            )
            for issue in issues
        ]
        for fut in as_completed(futures):
            try:
                rows.append(fut.result())
            except Exception as e:
                logger.warning("changelog failed: %s", e)
    return rows


def parse_outlier_sigma(override: float | None = None, *, disable: bool = False) -> float:
    """Upper-tail cutoff in standard deviations (``0`` = disabled). Default ``4``."""
    if disable:
        return 0.0
    if override is not None:
        return max(0.0, float(override))
    raw = (os.environ.get("BPO_CYCLE_TIME_OUTLIER_SIGMA") or "4").strip()
    if raw.lower() in ("0", "false", "no", "off", "none"):
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 4.0


def drop_upper_outliers(
    measured: list[tuple[str, float]],
    *,
    sigma: float,
) -> tuple[list[tuple[str, float]], list[dict[str, Any]], float | None]:
    """Drop tickets with cycle time above ``mean + sigma * stdev`` (upper tail only)."""
    if sigma <= 0 or len(measured) < 3:
        return measured, [], None
    vals = [d for _, d in measured]
    mean = statistics.mean(vals)
    stdev = statistics.stdev(vals)
    if stdev <= 0:
        return measured, [], None
    cutoff = mean + sigma * stdev
    kept: list[tuple[str, float]] = []
    dropped: list[dict[str, Any]] = []
    for key, days in measured:
        if days > cutoff:
            dropped.append(
                {
                    "key": key,
                    "cycle_days": round(days, 2),
                    "cutoff_days": round(cutoff, 2),
                    "reason": f">{sigma}σ above mean",
                }
            )
        else:
            kept.append((key, days))
    return kept, dropped, round(cutoff, 2)


def summarize_cycle_times(days_list: list[float]) -> dict[str, float | None]:
    if not days_list:
        return {
            "median_days": None,
            "mean_days": None,
            "p85_days": None,
            "min_days": None,
            "max_days": None,
        }
    sorted_days = sorted(days_list)
    n = len(sorted_days)
    p85_idx = max(0, min(n - 1, int(round(0.85 * (n - 1)))))
    return {
        "median_days": round(statistics.median(sorted_days), 2),
        "mean_days": round(statistics.mean(sorted_days), 2),
        "p85_days": round(sorted_days[p85_idx], 2),
        "min_days": round(sorted_days[0], 2),
        "max_days": round(sorted_days[-1], 2),
    }


def board_cycle_time_summary(
    client: JiraClient,
    board: dict[str, Any],
    *,
    days: int = 30,
    max_issues: int = 500,
    workers: int = 6,
    timeout: float = 60.0,
    status_map: dict[str, str] | None = None,
    excluded_issue_types: tuple[str, ...] | None = None,
    outlier_sigma: float | None = None,
    disable_outlier_filter: bool = False,
) -> dict[str, Any]:
    """Compute cycle-time stats for one board."""
    board_id = int(board["board_id"])
    team_label = str(board.get("team_label") or board.get("name") or board_id)
    t0 = time.time()

    excluded = (
        parse_excluded_issue_types()
        if excluded_issue_types is None
        else excluded_issue_types
    )
    smap = status_map or load_status_category_map(client, timeout=timeout)
    issues, reported_total, jql = _fetch_board_done_issues(
        client,
        board_id,
        days=days,
        max_issues=max_issues,
        excluded_issue_types=excluded,
        timeout=timeout,
    )

    issue_rows = _compute_issue_cycle_rows(
        client, issues, status_map=smap, workers=workers, timeout=timeout
    )
    sigma = parse_outlier_sigma(outlier_sigma, disable=disable_outlier_filter)
    skipped_no_active = 0
    measured_raw: list[tuple[str, float]] = []
    for row in issue_rows:
        if row.skipped_no_active:
            skipped_no_active += 1
            continue
        if row.cycle_days is not None:
            measured_raw.append((row.key, row.cycle_days))

    measured_kept, outliers, cutoff = drop_upper_outliers(measured_raw, sigma=sigma)
    cycle_days = [d for _, d in measured_kept]
    per_issue = [{"key": k, "cycle_days": round(d, 2)} for k, d in measured_kept]
    per_issue.sort(key=lambda x: x.get("cycle_days") or 0, reverse=True)
    stats = summarize_cycle_times(cycle_days)

    summary = BoardCycleTimeSummary(
        board_id=board_id,
        team_label=team_label,
        board_name=str(board.get("name") or ""),
        project_key=str(board.get("project_key") or ""),
        window_days=days,
        completed_count=len(issues),
        measured_count=len(cycle_days),
        skipped_no_active=skipped_no_active,
        **stats,
    )

    truncated = reported_total is not None and reported_total > len(issues)

    return {
        "team": team_label,
        "board_id": board_id,
        "board_name": summary.board_name,
        "project_key": summary.project_key,
        "window_days": days,
        "jql": jql,
        "excluded_issue_types": list(excluded),
        "outlier_sigma": sigma if sigma > 0 else None,
        "outlier_cutoff_days": cutoff,
        "outliers_dropped": len(outliers),
        "outlier_issues": outliers[:20],
        "completed_in_window": len(issues),
        "reported_total": reported_total,
        "truncated": truncated,
        "measured": summary.measured_count,
        "measured_before_outliers": len(measured_raw),
        "skipped_no_in_progress": skipped_no_active,
        "median_days": summary.median_days,
        "mean_days": summary.mean_days,
        "p85_days": summary.p85_days,
        "min_days": summary.min_days,
        "max_days": summary.max_days,
        "cycle_time_definition": (
            "Calendar days summed while status category is In Progress "
            f"({ _ACTIVE_CATEGORY }) until first Done"
        ),
        "elapsed_seconds": round(time.time() - t0, 1),
        "top_issues": per_issue[:10],
        "issues": per_issue if len(per_issue) <= 50 else None,
    }


def _summarize_by_issue_type(
    rows: list[tuple[str, float, str]],
) -> list[dict[str, Any]]:
    """Median cycle time per ``issue_type`` from ``(type, days, month)`` tuples."""
    by_type: dict[str, list[float]] = defaultdict(list)
    for itype, days, _month in rows:
        by_type[itype].append(days)
    out: list[dict[str, Any]] = []
    for itype in sorted(by_type.keys()):
        vals = by_type[itype]
        stats = summarize_cycle_times(vals)
        out.append({"issue_type": itype, "count": len(vals), **stats})
    out.sort(key=lambda x: int(x.get("count") or 0), reverse=True)
    return out


def board_cycle_time_history(
    client: JiraClient,
    board: dict[str, Any],
    *,
    months: int = 12,
    max_issues: int = 2000,
    workers: int = 6,
    timeout: float = 60.0,
    status_map: dict[str, str] | None = None,
    excluded_issue_types: tuple[str, ...] | None = None,
    outlier_sigma: float | None = None,
    disable_outlier_filter: bool = False,
    include_issues: bool = False,
) -> dict[str, Any]:
    """Monthly median/mean cycle time for issues resolved in the last *months* calendar months."""
    board_id = int(board["board_id"])
    team_label = str(board.get("team_label") or board.get("name") or board_id)
    window_days = history_window_days(months)
    periods = trailing_month_periods(months)
    t0 = time.time()

    excluded = (
        parse_excluded_issue_types()
        if excluded_issue_types is None
        else excluded_issue_types
    )
    smap = status_map or load_status_category_map(client, timeout=timeout)
    issues, reported_total, jql = _fetch_board_done_issues(
        client,
        board_id,
        days=window_days,
        max_issues=max_issues,
        excluded_issue_types=excluded,
        timeout=timeout,
    )
    issue_rows = _compute_issue_cycle_rows(
        client, issues, status_map=smap, workers=workers, timeout=timeout
    )

    sigma = parse_outlier_sigma(outlier_sigma, disable=disable_outlier_filter)
    period_set = set(periods)
    completed_by_month: dict[str, int] = defaultdict(int)
    skipped_by_month: dict[str, int] = defaultdict(int)
    measured_raw: list[tuple[str, float, str, str]] = []

    for row in issue_rows:
        month = row.resolved_month
        if month not in period_set:
            continue
        completed_by_month[month] += 1
        if row.skipped_no_active:
            skipped_by_month[month] += 1
            continue
        if row.cycle_days is not None:
            measured_raw.append((row.key, row.cycle_days, month, row.issue_type))

    pairs = [(k, d) for k, d, _m, _t in measured_raw]
    kept_pairs, outliers, cutoff = drop_upper_outliers(pairs, sigma=sigma)
    kept_keys = {k for k, _ in kept_pairs}
    outlier_keys = {o["key"] for o in outliers}
    outlier_by_month: dict[str, int] = defaultdict(int)
    for key, _d, month, _t in measured_raw:
        if key in outlier_keys:
            outlier_by_month[month] += 1

    cycle_by_month: dict[str, list[float]] = defaultdict(list)
    kept_for_type: list[tuple[str, float, str]] = []
    issues_detail: list[dict[str, Any]] = []
    for key, days, month, itype in measured_raw:
        is_outlier = key in outlier_keys
        if include_issues or is_outlier:
            issues_detail.append(
                {
                    "key": key,
                    "cycle_days": round(days, 2),
                    "resolved_month": month,
                    "issue_type": itype,
                    "outlier": is_outlier,
                }
            )
        if key not in kept_keys:
            continue
        cycle_by_month[month].append(days)
        kept_for_type.append((itype, days, month))

    history: list[dict[str, Any]] = []
    for period in periods:
        days_list = cycle_by_month.get(period, [])
        stats = summarize_cycle_times(days_list)
        history.append(
            {
                "period": period,
                "completed": completed_by_month.get(period, 0),
                "measured": len(days_list),
                "outliers_dropped": outlier_by_month.get(period, 0),
                "skipped_no_in_progress": skipped_by_month.get(period, 0),
                **stats,
            }
        )

    all_measured = [d for vals in cycle_by_month.values() for d in vals]
    overall = summarize_cycle_times(all_measured)

    return {
        "team": team_label,
        "board_id": board_id,
        "board_name": str(board.get("name") or ""),
        "project_key": str(board.get("project_key") or ""),
        "months": months,
        "window_days": window_days,
        "periods": periods,
        "jql": jql,
        "excluded_issue_types": list(excluded),
        "outlier_sigma": sigma if sigma > 0 else None,
        "outlier_cutoff_days": cutoff,
        "outliers_dropped": len(outliers),
        "outlier_issues": outliers[:20],
        "measured_before_outliers": len(measured_raw),
        "completed_in_window": len(issues),
        "reported_total": reported_total,
        "truncated": reported_total is not None and reported_total > len(issues),
        "measured_total": len(all_measured),
        "by_issue_type": _summarize_by_issue_type(kept_for_type),
        "history": history,
        "overall": overall,
        "issues": issues_detail if include_issues else None,
        "outlier_issues_full": outliers if not include_issues else None,
        "cycle_time_definition": (
            "Calendar days summed while status category is In Progress "
            f"({_ACTIVE_CATEGORY}) until first Done; bucketed by resolution month"
        ),
        "elapsed_seconds": round(time.time() - t0, 1),
    }


def get_dev_team_cycle_times(
    client: JiraClient,
    *,
    board_ids: list[int] | None = None,
    days: int = 30,
    months: int | None = None,
    max_issues_per_board: int = 500,
    workers: int = 6,
    timeout: float = 60.0,
    excluded_issue_types: tuple[str, ...] | None = None,
    include_all_issue_types: bool = False,
    outlier_sigma: float | None = None,
    disable_outlier_filter: bool = False,
    include_issues: bool = False,
) -> dict[str, Any]:
    """Cycle-time payload for default or selected development boards."""
    boards = list(DEV_CYCLE_TIME_BOARDS)
    if board_ids:
        wanted = {int(b) for b in board_ids}
        boards = [b for b in boards if int(b["board_id"]) in wanted]
        missing = wanted - {int(b["board_id"]) for b in boards}
        if missing:
            return {
                "error": f"unknown board id(s): {sorted(missing)}; "
                f"configured: {[b['board_id'] for b in DEV_CYCLE_TIME_BOARDS]}",
                "teams": [],
            }
    if not boards:
        return {"error": "no boards selected", "teams": []}

    excluded = parse_excluded_issue_types(
        excluded_issue_types,
        include_all=include_all_issue_types,
    )

    try:
        status_map = load_status_category_map(client, timeout=timeout)
    except Exception as e:
        return {"error": f"failed to load Jira statuses: {e}", "teams": []}

    if months is not None and int(months) > 0:
        hist_max = history_fetch_cap(int(months), max_issues_per_board)
        teams_hist: list[dict[str, Any]] = []
        for board in boards:
            try:
                teams_hist.append(
                    board_cycle_time_history(
                        client,
                        board,
                        months=int(months),
                        max_issues=hist_max,
                        workers=workers,
                        timeout=timeout,
                        status_map=status_map,
                        excluded_issue_types=excluded,
                        outlier_sigma=outlier_sigma,
                        disable_outlier_filter=disable_outlier_filter,
                        include_issues=include_issues,
                    )
                )
            except Exception as e:
                teams_hist.append(
                    {
                        "team": board.get("team_label"),
                        "board_id": board.get("board_id"),
                        "error": str(e),
                    }
                )
        sigma = parse_outlier_sigma(outlier_sigma, disable=disable_outlier_filter)
        return {
            "mode": "history",
            "months": int(months),
            "window_days": history_window_days(int(months)),
            "excluded_issue_types": list(excluded),
            "outlier_sigma": sigma if sigma > 0 else None,
            "max_issues_per_board": hist_max,
            "boards": [b["board_id"] for b in boards],
            "teams": teams_hist,
        }

    teams: list[dict[str, Any]] = []
    for board in boards:
        try:
            teams.append(
                board_cycle_time_summary(
                    client,
                    board,
                    days=days,
                    max_issues=max_issues_per_board,
                    workers=workers,
                    timeout=timeout,
                    status_map=status_map,
                    excluded_issue_types=excluded,
                    outlier_sigma=outlier_sigma,
                    disable_outlier_filter=disable_outlier_filter,
                )
            )
        except Exception as e:
            teams.append(
                {
                    "team": board.get("team_label"),
                    "board_id": board.get("board_id"),
                    "error": str(e),
                }
            )

    sigma = parse_outlier_sigma(outlier_sigma, disable=disable_outlier_filter)
    return {
        "mode": "snapshot",
        "window_days": days,
        "excluded_issue_types": list(excluded),
        "outlier_sigma": sigma if sigma > 0 else None,
        "boards": [b["board_id"] for b in boards],
        "teams": teams,
    }
