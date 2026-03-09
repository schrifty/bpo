"""Quarter-aware date ranges for QBR deck generation.

Provides smart defaults:
  - If today is within GRACE_DAYS after a quarter end → use the *previous* quarter
  - If today is within EARLY_DAYS before a quarter end → use the *current* quarter (full)
  - Otherwise → use the current quarter (to-date)

All public functions return a QuarterRange with start, end, days, and a display label.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass

GRACE_DAYS = 21
EARLY_DAYS = 14

_Q_STARTS = [(1, 1), (4, 1), (7, 1), (10, 1)]


@dataclass(frozen=True)
class QuarterRange:
    label: str          # e.g. "Q1 2026"
    start: datetime.date
    end: datetime.date   # inclusive last day

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1


def quarter_of(d: datetime.date) -> tuple[int, int]:
    """Return (year, quarter_number) for a date."""
    return d.year, (d.month - 1) // 3 + 1


def quarter_start(year: int, q: int) -> datetime.date:
    month = (q - 1) * 3 + 1
    return datetime.date(year, month, 1)


def quarter_end(year: int, q: int) -> datetime.date:
    nxt = quarter_start(year, q) + datetime.timedelta(days=92)
    nxt_start = quarter_start(*quarter_of(nxt))
    return nxt_start - datetime.timedelta(days=1)


def prev_quarter(year: int, q: int) -> tuple[int, int]:
    if q == 1:
        return year - 1, 4
    return year, q - 1


def _make(year: int, q: int, end_override: datetime.date | None = None) -> QuarterRange:
    start = quarter_start(year, q)
    end = end_override or quarter_end(year, q)
    return QuarterRange(label=f"Q{q} {year}", start=start, end=end)


def resolve_quarter(
    override: str | None = None,
    today: datetime.date | None = None,
) -> QuarterRange:
    """Pick the best quarter range.

    Args:
        override: Explicit quarter like "Q1 2026", "Q4 2025", "prev", or "current".
                  None means auto-detect.
        today: Override today's date (for testing).
    """
    today = today or datetime.date.today()

    if override:
        ov = override.strip().lower()
        if ov == "prev":
            y, q = prev_quarter(*quarter_of(today))
            return _make(y, q)
        if ov == "current":
            y, q = quarter_of(today)
            return _make(y, q, end_override=today)
        ov_upper = override.strip().upper()
        if len(ov_upper) >= 2 and ov_upper[0] == "Q" and ov_upper[1].isdigit():
            parts = ov_upper.split()
            q_num = int(parts[0][1])
            yr = int(parts[1]) if len(parts) > 1 else today.year
            if not 1 <= q_num <= 4:
                raise ValueError(f"Invalid quarter: {override}")
            qe = quarter_end(yr, q_num)
            end = min(qe, today) if qe >= today else qe
            return _make(yr, q_num, end_override=end)
        raise ValueError(
            f"Invalid --quarter value: '{override}'. "
            "Use 'Q1 2026', 'prev', 'current', or omit for auto."
        )

    y, q = quarter_of(today)
    q_end = quarter_end(y, q)
    days_left = (q_end - today).days

    q_start = quarter_start(y, q)
    days_since_start = (today - q_start).days

    if days_since_start < GRACE_DAYS:
        py, pq = prev_quarter(y, q)
        pq_end = quarter_end(py, pq)
        if (today - pq_end).days <= GRACE_DAYS:
            return _make(py, pq)

    if days_left <= EARLY_DAYS:
        return _make(y, q, end_override=today)

    return _make(y, q, end_override=today)
