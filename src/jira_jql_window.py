"""Calendar trailing windows for Jira JQL (half-open UTC day bounds)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


def as_of_utc_date(as_of: date | datetime | None) -> date | None:
    if as_of is None:
        return None
    if isinstance(as_of, datetime):
        dt = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date()
    return as_of


def trailing_window_bounds(
    days: int,
    as_of: date | datetime | None,
) -> tuple[datetime, datetime]:
    """Half-open ``[start, end)`` covering *days* calendar days ending on *as_of*.

    *as_of* is the last included calendar day (UTC). ``end`` is midnight of the
    following day. When *as_of* is omitted, uses today UTC.
    """
    window = max(1, int(days))
    last = as_of_utc_date(as_of) or datetime.now(timezone.utc).date()
    end = datetime(last.year, last.month, last.day, tzinfo=timezone.utc) + timedelta(days=1)
    start = end - timedelta(days=window)
    return start, end


def jql_trailing_clause(
    field: str,
    days: int,
    as_of: date | datetime | None = None,
) -> str:
    """JQL for a trailing window.

    With no *as_of*, uses Jira relative ``field >= -Nd`` (live default). With
    *as_of*, uses ``field >= start AND field < end`` so history can be rebuilt.
    """
    window = max(1, int(days))
    if as_of is None:
        return f"{field} >= -{window}d"
    start, end = trailing_window_bounds(window, as_of)
    return (
        f'{field} >= "{start.strftime("%Y-%m-%d")}" '
        f'AND {field} < "{end.strftime("%Y-%m-%d")}"'
    )
