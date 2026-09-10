"""SQLite store for KPI observations (daily snapshot and month-close grains).

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

SCHEMA_VERSION = 1
GRAIN_DAILY = "daily"
GRAIN_MONTH = "month"
GRAINS = frozenset({GRAIN_DAILY, GRAIN_MONTH})

# Previous-calendar-month scorecard generators (period_key = YYYY-MM of that month).
MONTH_CLOSE_GENERATORS = frozenset(
    {
        "get_tokens_per_dev",
        "get_token_cost_per_dev",
        "get_prs_merged",
        "get_ai_assisted_prs_pct",
        "get_ai_code_share",
        "get_ai_automated_prs_pct",
        "get_ai_assisted_automated_prs_pct",
        "get_issues_shipped",
        "get_defects_per_100_issues",
        "get_defect_introduction_rate",
        "get_growth_allocation_pct",
        "get_ai_spend_pct",
        "get_ai_spend_per_issue",
        "get_headcount_plus_ai_spend_per_issue",
    }
)


def grain_for_generator(generator: str) -> str:
    name = (generator or "").strip()
    if name in MONTH_CLOSE_GENERATORS:
        return GRAIN_MONTH
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
    grain TEXT NOT NULL CHECK (grain IN ('daily', 'month')),
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
    PRIMARY KEY (metric_name, grain, period_key)
);

CREATE INDEX IF NOT EXISTS kpi_observation_period
    ON kpi_observation (grain, period_key DESC);
"""


class KPIStoreError(ValueError):
    """Invalid grain, period key, or store payload."""


@dataclass(frozen=True)
class StoredKPI:
    """One persisted KPI row plus the canonical observation."""

    metric_name: str
    grain: str
    period_key: str
    captured_at: str
    observation: KPIObservation
    generator: str | None = None
    tags: tuple[str, ...] = ()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_grain(grain: str) -> str:
    g = (grain or "").strip().lower()
    if g not in GRAINS:
        raise KPIStoreError(f"grain must be 'daily' or 'month', got {grain!r}")
    return g


def validate_period_key(grain: str, period_key: str) -> str:
    key = (period_key or "").strip()
    g = validate_grain(grain)
    if g == GRAIN_DAILY:
        if len(key) != 10 or key[4] != "-" or key[7] != "-":
            raise KPIStoreError(f"daily period_key must be YYYY-MM-DD, got {period_key!r}")
        try:
            datetime.strptime(key, "%Y-%m-%d")
        except ValueError as exc:
            raise KPIStoreError(f"invalid period_key {period_key!r} for grain {g!r}") from exc
        return key
    if len(key) != 7 or key[4] != "-":
        raise KPIStoreError(f"month period_key must be YYYY-MM, got {period_key!r}")
    try:
        datetime.strptime(key + "-01", "%Y-%m-%d")
    except ValueError as exc:
        raise KPIStoreError(f"invalid period_key {period_key!r} for grain {g!r}") from exc
    return key


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
    conn.executescript(_SCHEMA_SQL)
    conn.execute(
        "INSERT INTO kpi_store_meta(key, value) VALUES ('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()


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
    obs = KPIObservation(
        value=observation.value,
        numerator=observation.numerator,
        denominator=observation.denominator,
        as_of=observation.as_of or (pk if g == GRAIN_DAILY else pk + "-01"),
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


def upsert_kpi(conn: sqlite3.Connection, row: StoredKPI) -> None:
    """Insert or replace one observation (unique metric_name + grain + period_key)."""
    g = validate_grain(row.grain)
    pk = validate_period_key(g, row.period_key)
    obs = row.observation
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
            _json_dumps(obs.meta),
            obs.error,
            obs.as_of,
            obs.window_days,
        ),
    )
    conn.commit()


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
