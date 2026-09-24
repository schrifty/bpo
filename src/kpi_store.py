"""SQLite store for KPI observations at registry-defined grains.

The database file is local. :mod:`src.kpi_store_s3` uploads/downloads that file.
This module does not generate KPIs or talk to LeanDNA.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .kpi_observation import KPIObservation
from .metrics_registry import LEGACY_MONTHLY_GENERATORS

SCHEMA_VERSION = 3
GRAIN_HOURLY = "hourly"
GRAIN_DAILY = "daily"
GRAIN_WEEKLY = "weekly"
GRAIN_MONTHLY = "monthly"
GRAIN_QUARTERLY = "quarterly"
# Compatibility name used by older callers/tests.
GRAIN_MONTH = GRAIN_MONTHLY
GRAINS = frozenset(
    {GRAIN_HOURLY, GRAIN_DAILY, GRAIN_WEEKLY, GRAIN_MONTHLY, GRAIN_QUARTERLY}
)

# Headline month-close name. Retired daily open-stock rows used this name or
# ``Open Customer-Reported Bugs`` and are dropped on connect.
# Key under ``KPIObservation.meta`` describing an applied manual override.
OVERRIDE_META_KEY = "override"

CUSTOMER_REPORTED_BUGS_METRIC = "Customer-Reported Bugs"
_RETIRED_OPEN_CUSTOMER_REPORTED_BUGS_METRIC = "Open Customer-Reported Bugs"
_RETIRED_COMBINED_ESCALATION_RATE_METRIC = "Escalation Rate (30 Days)"
_RETIRED_TRAILING_SUPPORT_METRICS = (
    "HELP Resolved / Created (30 Days)",
    "Engineering Escalation Rate (30 Days)",
    "Data Escalation Rate (30 Days)",
)

# Previous-calendar-month scorecard generators (period_key = YYYY-MM of that month).
MONTH_CLOSE_GENERATORS = LEGACY_MONTHLY_GENERATORS


def grain_for_generator(generator: str) -> str:
    """Legacy fallback for registry rows that predate the required grain field."""
    name = (generator or "").strip()
    if name in MONTH_CLOSE_GENERATORS:
        return GRAIN_MONTHLY
    return GRAIN_DAILY


def default_kpi_store_path() -> Path:
    from .config import CORTEX_CACHE_ROOT

    return Path(CORTEX_CACHE_ROOT) / "kpi" / "observations.sqlite"

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS kpi_store_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kpi_observation (
    metric_name TEXT NOT NULL,
    grain TEXT NOT NULL CHECK (
        grain IN ('hourly', 'daily', 'weekly', 'monthly', 'quarterly')
    ),
    period_key TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    value REAL,
    numerator REAL,
    denominator REAL,
    generator TEXT,
    tags_json TEXT NOT NULL DEFAULT '[]',
    meta_json TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    as_of TEXT,
    window_days INTEGER,
    override_value REAL,
    override_by TEXT,
    override_at TEXT,
    PRIMARY KEY (metric_name, grain, period_key)
);

CREATE INDEX IF NOT EXISTS kpi_observation_period
    ON kpi_observation (grain, period_key DESC);
"""


class KPIStoreError(ValueError):
    """Invalid grain, period key, or store payload."""


@dataclass(frozen=True)
class ManualOverride:
    """A human-entered value that shadows — but never replaces — the stored reading."""

    value: float
    by: str | None = None
    at: str | None = None


@dataclass(frozen=True)
class StoredKPI:
    """One persisted KPI row plus the canonical observation.

    ``observation`` is always the captured (generator) reading. When a manual
    override exists, read ``effective_observation`` for the number to display.
    """

    metric_name: str
    grain: str
    period_key: str
    captured_at: str
    observation: KPIObservation
    generator: str | None = None
    tags: tuple[str, ...] = ()
    override: ManualOverride | None = None

    @property
    def is_overridden(self) -> bool:
        return self.override is not None

    @property
    def effective_observation(self) -> KPIObservation:
        """Observation to display: the override when set, else the stored reading.

        The generated value stays available under ``meta[OVERRIDE_META_KEY]`` so
        callers can show both.
        """
        if self.override is None:
            return self.observation
        meta = {
            k: v for k, v in self.observation.meta.items() if k != OVERRIDE_META_KEY
        }
        meta[OVERRIDE_META_KEY] = {
            "value": self.override.value,
            "by": self.override.by,
            "at": self.override.at,
            "generated_value": self.observation.display_value,
            "generated_error": self.observation.error,
        }
        return KPIObservation(
            value=self.override.value,
            as_of=self.observation.as_of,
            window_days=self.observation.window_days,
            source=self.observation.source + ("kpi-web-override",),
            warnings=self.observation.warnings,
            origin="stored",
            meta=meta,
        )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_grain(grain: str) -> str:
    g = (grain or "").strip().lower()
    if g == "month":
        g = GRAIN_MONTHLY
    if g not in GRAINS:
        raise KPIStoreError(f"grain must be one of {sorted(GRAINS)}, got {grain!r}")
    return g


def validate_period_key(grain: str, period_key: str) -> str:
    key = (period_key or "").strip()
    g = validate_grain(grain)
    if g == GRAIN_HOURLY:
        try:
            datetime.strptime(key, "%Y-%m-%dT%H")
        except ValueError as exc:
            raise KPIStoreError(
                f"hourly period_key must be YYYY-MM-DDTHH, got {period_key!r}"
            ) from exc
        return key
    if g == GRAIN_DAILY:
        if len(key) != 10 or key[4] != "-" or key[7] != "-":
            raise KPIStoreError(f"daily period_key must be YYYY-MM-DD, got {period_key!r}")
        try:
            datetime.strptime(key, "%Y-%m-%d")
        except ValueError as exc:
            raise KPIStoreError(f"invalid period_key {period_key!r} for grain {g!r}") from exc
        return key
    if g == GRAIN_WEEKLY:
        try:
            datetime.strptime(f"{key}-1", "%G-W%V-%u")
        except ValueError as exc:
            raise KPIStoreError(
                f"weekly period_key must be YYYY-Www, got {period_key!r}"
            ) from exc
        return key
    if g == GRAIN_MONTHLY:
        try:
            datetime.strptime(key + "-01", "%Y-%m-%d")
        except ValueError as exc:
            raise KPIStoreError(
                f"monthly period_key must be YYYY-MM, got {period_key!r}"
            ) from exc
        return key
    if g == GRAIN_QUARTERLY:
        if (
            len(key) != 7
            or key[4:6] != "-Q"
            or key[-1] not in {"1", "2", "3", "4"}
        ):
            raise KPIStoreError(
                f"quarterly period_key must be YYYY-Q1..Q4, got {period_key!r}"
            )
        try:
            int(key[:4])
        except ValueError as exc:
            raise KPIStoreError(
                f"quarterly period_key must be YYYY-Q1..Q4, got {period_key!r}"
            ) from exc
        return key
    raise KPIStoreError(f"unsupported grain {g!r}")


def _as_sql_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


def connect(path: str | Path) -> sqlite3.Connection:
    """Open (or create) the SQLite file and apply schema."""
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    init_schema(conn)
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    _migrate_grain_schema(conn)
    conn.executescript(_SCHEMA_SQL)
    _migrate_override_columns(conn)
    conn.execute(
        "INSERT INTO kpi_store_meta(key, value) VALUES ('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (str(SCHEMA_VERSION),),
    )
    migrate_legacy_daily_customer_reported_bugs(conn)
    migrate_retired_combined_escalation_rate(conn)
    migrate_retired_trailing_support_metrics(conn)
    dedupe_one_reading_per_day(conn)
    conn.commit()


def _migrate_grain_schema(conn: sqlite3.Connection) -> None:
    """Rebuild v1's daily/month table so all registry grains are accepted."""
    found = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'kpi_observation'"
    ).fetchone()
    if found is None:
        return
    sql = str(found[0] or "")
    if "'hourly'" in sql and "'quarterly'" in sql:
        return
    conn.execute("ALTER TABLE kpi_observation RENAME TO kpi_observation_v1")
    conn.execute("DROP INDEX IF EXISTS kpi_observation_period")
    conn.executescript(_SCHEMA_SQL)
    conn.execute(
        """
        INSERT INTO kpi_observation (
            metric_name, grain, period_key, captured_at,
            value, numerator, denominator, generator,
            tags_json, meta_json, error, as_of, window_days
        )
        SELECT
            metric_name,
            CASE grain WHEN 'month' THEN 'monthly' ELSE grain END,
            period_key, captured_at,
            value, numerator, denominator, generator,
            tags_json, meta_json, error, as_of, window_days
        FROM kpi_observation_v1
        """
    )
    conn.execute("DROP TABLE kpi_observation_v1")


def _migrate_override_columns(conn: sqlite3.Connection) -> None:
    """Add v3 manual-override columns to a v2 table (values are left untouched)."""
    have = {str(row["name"]) for row in conn.execute("PRAGMA table_info(kpi_observation)")}
    for column, decl in (
        ("override_value", "REAL"),
        ("override_by", "TEXT"),
        ("override_at", "TEXT"),
    ):
        if column not in have:
            conn.execute(f"ALTER TABLE kpi_observation ADD COLUMN {column} {decl}")


def migrate_legacy_daily_customer_reported_bugs(conn: sqlite3.Connection) -> None:
    """Drop retired daily open-bug stock (not a registry KPI)."""
    conn.execute(
        """
        DELETE FROM kpi_observation
        WHERE grain = ? AND metric_name IN (?, ?)
        """,
        (
            GRAIN_DAILY,
            _RETIRED_OPEN_CUSTOMER_REPORTED_BUGS_METRIC,
            CUSTOMER_REPORTED_BUGS_METRIC,
        ),
    )


def migrate_retired_combined_escalation_rate(conn: sqlite3.Connection) -> None:
    """Drop the former combined HELP→LEAN+CUSTOMER escalation rate KPI."""
    conn.execute(
        "DELETE FROM kpi_observation WHERE metric_name = ?",
        (_RETIRED_COMBINED_ESCALATION_RATE_METRIC,),
    )


def migrate_retired_trailing_support_metrics(conn: sqlite3.Connection) -> None:
    """Drop trailing-30d support KPI names replaced by month-close rows."""
    conn.execute(
        f"DELETE FROM kpi_observation WHERE metric_name IN ({','.join('?' * len(_RETIRED_TRAILING_SUPPORT_METRICS))})",
        _RETIRED_TRAILING_SUPPORT_METRICS,
    )


def _observation_calendar_day(*, as_of: str | None, period_key: str, grain: str) -> str:
    """YYYY-MM-DD used to enforce one reading per KPI per day."""
    raw = (as_of or period_key or "").strip()
    day = raw[:10]
    if len(day) == 10 and day[4] == "-" and day[7] == "-":
        return day
    if grain == GRAIN_MONTHLY and len((period_key or "").strip()) == 7:
        return f"{period_key.strip()}-01"
    return day or (period_key or "").strip()


def _dedupe_keep_rank(row: sqlite3.Row) -> tuple[int, int, str, str]:
    """Higher tuple wins: manual override, period matching the day, later capture, later period."""
    grain = str(row["grain"] or "")
    period = str(row["period_key"] or "")
    day = _observation_calendar_day(as_of=row["as_of"], period_key=period, grain=grain)
    if grain == GRAIN_DAILY:
        matches = 1 if period == day else 0
    elif grain == GRAIN_MONTHLY:
        matches = 1 if period == day[:7] else 0
    else:
        matches = 0
    overridden = 1 if row["override_value"] is not None else 0
    return (overridden, matches, str(row["captured_at"] or ""), period)


def dedupe_one_reading_per_day(conn: sqlite3.Connection) -> int:
    """Delete extra rows so each metric+grain has at most one reading per calendar day.

    Day is ``as_of`` (YYYY-MM-DD) when set, else ``period_key``. Among duplicates,
    keep the row whose period is that day when possible, otherwise the latest
    ``captured_at`` (then latest ``period_key``).
    """
    grouped: dict[tuple[str, str, str], list[sqlite3.Row]] = {}
    for raw in conn.execute(
        "SELECT metric_name, grain, period_key, as_of, captured_at, override_value "
        "FROM kpi_observation"
    ):
        day = _observation_calendar_day(
            as_of=raw["as_of"], period_key=raw["period_key"], grain=raw["grain"]
        )
        grouped.setdefault((raw["metric_name"], raw["grain"], day), []).append(raw)
    deleted = 0
    for (_name, grain, _day), rows in grouped.items():
        if len(rows) < 2:
            continue
        winner = max(rows, key=_dedupe_keep_rank)
        for row in rows:
            if (
                row["metric_name"] == winner["metric_name"]
                and row["grain"] == winner["grain"]
                and row["period_key"] == winner["period_key"]
            ):
                continue
            conn.execute(
                "DELETE FROM kpi_observation WHERE metric_name = ? AND grain = ? AND period_key = ?",
                (row["metric_name"], grain, row["period_key"]),
            )
            deleted += 1
    return deleted


def stored_kpi_from_observation(
    *,
    metric_name: str,
    grain: str,
    period_key: str,
    observation: KPIObservation,
    generator: str | None = None,
    tags: Iterable[str] | None = None,
    captured_at: str | None = None,
) -> StoredKPI:
    """Build a store row from a live (or stored) observation."""
    name = (metric_name or "").strip()
    if not name:
        raise KPIStoreError("metric_name is required")
    g = validate_grain(grain)
    pk = validate_period_key(g, period_key)
    tag_tuple = tuple(str(t).strip() for t in (tags or ()) if str(t).strip())
    as_of = _as_of_for_period(g, pk)
    obs = KPIObservation(
        value=observation.value,
        numerator=observation.numerator,
        denominator=observation.denominator,
        as_of=as_of,
        window_days=observation.window_days,
        source=observation.source,
        warnings=observation.warnings,
        error=observation.error,
        origin="stored",
        meta=dict(observation.meta),
    )
    return StoredKPI(
        metric_name=name,
        grain=g,
        period_key=pk,
        captured_at=captured_at or _utc_now_iso(),
        observation=obs,
        generator=(generator or "").strip() or None,
        tags=tag_tuple,
    )


def period_key_now(grain: str, *, when: datetime | None = None) -> str:
    """Calendar period key for *grain* at *when* (UTC now if omitted)."""
    stamp = when if when is not None else datetime.now(timezone.utc)
    g = validate_grain(grain)
    if g == GRAIN_HOURLY:
        return stamp.strftime("%Y-%m-%dT%H")
    day = stamp.date()
    if g == GRAIN_DAILY:
        return day.isoformat()
    if g == GRAIN_WEEKLY:
        iso = day.isocalendar()
        return f"{iso.year:04d}-W{iso.week:02d}"
    if g == GRAIN_MONTHLY:
        return f"{day.year:04d}-{day.month:02d}"
    if g == GRAIN_QUARTERLY:
        quarter = (day.month - 1) // 3 + 1
        return f"{day.year:04d}-Q{quarter}"
    raise KPIStoreError(f"unsupported grain {g!r}")


def _override_target_period(
    conn: sqlite3.Connection,
    *,
    metric_name: str,
    grain: str,
    period_key: str | None,
) -> tuple[str, StoredKPI | None]:
    """Resolve which period an override applies to: explicit, latest stored, or now."""
    if period_key and str(period_key).strip():
        pk = validate_period_key(grain, str(period_key).strip())
        return pk, get_kpi(conn, metric_name, grain, pk)
    rows = list_kpis(conn, metric_name=metric_name, grain=grain, limit=1)
    if rows:
        return rows[0].period_key, rows[0]
    return period_key_now(grain), None


def set_manual_override(
    conn: sqlite3.Connection,
    *,
    metric_name: str,
    grain: str,
    value: float,
    period_key: str | None = None,
    tags: Iterable[str] | None = None,
    generator: str | None = None,
    actor: str | None = None,
) -> StoredKPI:
    """Record a manual value for a period without touching the generated reading.

    Targets the given period, else the latest stored one, else the current
    period. When no row exists yet the reading stays empty and only the
    override carries a number.
    """
    name = (metric_name or "").strip()
    if not name:
        raise KPIStoreError("metric_name is required")
    g = validate_grain(grain)
    pk, existing = _override_target_period(
        conn, metric_name=name, grain=g, period_key=period_key
    )
    if existing is None:
        upsert_kpi(
            conn,
            stored_kpi_from_observation(
                metric_name=name,
                grain=g,
                period_key=pk,
                observation=KPIObservation(
                    origin="stored",
                    warnings=("no generated reading for this period",),
                ),
                generator=generator,
                tags=tags or (),
            ),
        )
    conn.execute(
        """
        UPDATE kpi_observation
        SET override_value = ?, override_by = ?, override_at = ?
        WHERE metric_name = ? AND grain = ? AND period_key = ?
        """,
        (float(value), (actor or "").strip() or None, _utc_now_iso(), name, g, pk),
    )
    conn.commit()
    written = get_kpi(conn, name, g, pk)
    if written is None or written.override is None:
        raise KPIStoreError(f"failed to persist override for {name!r} {g} {pk}")
    return written


def clear_manual_override(
    conn: sqlite3.Connection,
    *,
    metric_name: str,
    grain: str,
    period_key: str | None = None,
) -> StoredKPI | None:
    """Drop the manual override so the generated reading shows again."""
    name = (metric_name or "").strip()
    if not name:
        raise KPIStoreError("metric_name is required")
    g = validate_grain(grain)
    pk, existing = _override_target_period(
        conn, metric_name=name, grain=g, period_key=period_key
    )
    if existing is None:
        raise KPIStoreError(f"no stored reading to clear for {name!r} {g} {pk}")
    conn.execute(
        """
        UPDATE kpi_observation
        SET override_value = NULL, override_by = NULL, override_at = NULL
        WHERE metric_name = ? AND grain = ? AND period_key = ?
        """,
        (name, g, pk),
    )
    conn.commit()
    return get_kpi(conn, name, g, pk)


def _as_of_for_period(grain: str, period_key: str) -> str:
    """Canonical date/time representing a persisted grain period."""
    if grain == GRAIN_HOURLY:
        return f"{period_key}:00:00Z"
    if grain == GRAIN_DAILY:
        return period_key
    if grain == GRAIN_WEEKLY:
        return datetime.strptime(f"{period_key}-1", "%G-W%V-%u").date().isoformat()
    if grain == GRAIN_MONTHLY:
        return f"{period_key}-01"
    if grain == GRAIN_QUARTERLY:
        year = int(period_key[:4])
        quarter = int(period_key[-1])
        return f"{year:04d}-{((quarter - 1) * 3) + 1:02d}-01"
    raise KPIStoreError(f"unsupported grain {grain!r}")


def upsert_kpi(conn: sqlite3.Connection, row: StoredKPI) -> None:
    """Insert or replace one captured observation.

    Unique on metric_name + grain + period_key. Any manual override on the row
    is preserved: a generator re-run updates the reading, never the override.
    """
    g = validate_grain(row.grain)
    pk = validate_period_key(g, row.period_key)
    obs = row.observation
    meta = {k: v for k, v in obs.meta.items() if k != OVERRIDE_META_KEY}
    conn.execute(
        """
        INSERT INTO kpi_observation (
            metric_name, grain, period_key, captured_at,
            value, numerator, denominator, generator,
            tags_json, meta_json, error, as_of, window_days
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(metric_name, grain, period_key) DO UPDATE SET
            captured_at = excluded.captured_at,
            value = excluded.value,
            numerator = excluded.numerator,
            denominator = excluded.denominator,
            generator = excluded.generator,
            tags_json = excluded.tags_json,
            meta_json = excluded.meta_json,
            error = excluded.error,
            as_of = excluded.as_of,
            window_days = excluded.window_days
        """,
        (
            row.metric_name,
            g,
            pk,
            row.captured_at,
            _as_sql_float(obs.value),
            _as_sql_float(obs.numerator),
            _as_sql_float(obs.denominator),
            row.generator,
            _json_dumps(list(row.tags)),
            _json_dumps(meta),
            obs.error,
            obs.as_of,
            obs.window_days,
        ),
    )
    conn.commit()


def _row_override(raw: sqlite3.Row) -> ManualOverride | None:
    if "override_value" not in raw.keys():
        return None
    value = raw["override_value"]
    if value is None:
        return None
    return ManualOverride(
        value=float(value),
        by=raw["override_by"],
        at=raw["override_at"],
    )


def _row_to_stored(raw: sqlite3.Row) -> StoredKPI:
    tags_raw = json.loads(raw["tags_json"] or "[]")
    tags = tuple(str(t) for t in tags_raw) if isinstance(tags_raw, list) else ()
    meta_raw = json.loads(raw["meta_json"] or "{}")
    meta = meta_raw if isinstance(meta_raw, dict) else {}
    obs = KPIObservation(
        value=raw["value"],
        numerator=raw["numerator"],
        denominator=raw["denominator"],
        as_of=raw["as_of"],
        window_days=raw["window_days"],
        source=("kpi-store",),
        error=raw["error"],
        origin="stored",
        meta=meta,
    )
    return StoredKPI(
        metric_name=raw["metric_name"],
        grain=raw["grain"],
        period_key=raw["period_key"],
        captured_at=raw["captured_at"],
        observation=obs,
        generator=raw["generator"],
        tags=tags,
        override=_row_override(raw),
    )


def get_kpi(
    conn: sqlite3.Connection,
    metric_name: str,
    grain: str,
    period_key: str,
) -> StoredKPI | None:
    g = validate_grain(grain)
    pk = validate_period_key(g, period_key)
    cur = conn.execute(
        "SELECT * FROM kpi_observation WHERE metric_name = ? AND grain = ? AND period_key = ?",
        (metric_name, g, pk),
    )
    found = cur.fetchone()
    return _row_to_stored(found) if found else None


def list_kpis(
    conn: sqlite3.Connection,
    *,
    metric_name: str | None = None,
    grain: str | None = None,
    limit: int | None = None,
) -> list[StoredKPI]:
    """Newest ``period_key`` first, then metric name."""
    clauses: list[str] = []
    args: list[Any] = []
    if metric_name:
        clauses.append("metric_name = ?")
        args.append(metric_name)
    if grain is not None:
        clauses.append("grain = ?")
        args.append(validate_grain(grain))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = (
        f"SELECT * FROM kpi_observation {where} "
        "ORDER BY period_key DESC, metric_name ASC"
    )
    if limit is not None:
        sql += " LIMIT ?"
        args.append(int(limit))
    return [_row_to_stored(r) for r in conn.execute(sql, args)]
