"""Health Score observation store.

Columns mirror ``kpi_observation`` in :mod:`src.kpi_store` (grain, period_key,
captured_at, value, generator, tags/meta JSON, error, as_of, override_*) so the
two stores can merge later. Health Score adds the entity dimension and the
weighted ``points`` the framework scores on.

Like the KPI store, a manual entry is an **override**: it shadows the generated
reading on screen and never overwrites it.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.kpi_store import GRAINS

SCHEMA = """
CREATE TABLE IF NOT EXISTS healthscore_observation (
    entity_id TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    grain TEXT NOT NULL CHECK (
        grain IN ('hourly', 'daily', 'weekly', 'monthly', 'quarterly')
    ),
    period_key TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    value REAL,
    points REAL,
    generator TEXT,
    tags_json TEXT NOT NULL DEFAULT '[]',
    meta_json TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    as_of TEXT,
    override_value REAL,
    override_points REAL,
    override_note TEXT,
    override_by TEXT,
    override_at TEXT,
    PRIMARY KEY (entity_id, metric_name, grain, period_key)
);
CREATE INDEX IF NOT EXISTS healthscore_observation_period
ON healthscore_observation (entity_id, grain, period_key DESC);
"""


class HealthScoreStoreError(ValueError):
    """Invalid grain, period key, or store payload."""


def default_store_path() -> Path:
    from src.config import CORTEX_CACHE_ROOT

    return Path(CORTEX_CACHE_ROOT) / "healthscore" / "observations.sqlite"


def connect(path: Path | None = None) -> sqlite3.Connection:
    target = path or default_store_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    _ensure_current_schema(conn)
    conn.executescript(SCHEMA)
    return conn


def _ensure_current_schema(conn: sqlite3.Connection) -> None:
    """Replace the pre-KPI-alignment table. CREATE TABLE IF NOT EXISTS cannot add columns."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='healthscore_observation'"
    ).fetchone()
    if row is None:
        return
    cols = {item[1] for item in conn.execute("PRAGMA table_info(healthscore_observation)")}
    if "grain" in cols and "metric_name" in cols:
        return
    conn.execute("DROP TABLE healthscore_observation")
    conn.commit()


def period_key_for(grain: str, as_of: date) -> str:
    """Same period keys the KPI snapshot writes, so merged history lines up."""
    from src.kpi_snapshot import period_key_for as kpi_period_key_for

    return kpi_period_key_for(_checked_grain(grain), as_of)


def _checked_grain(grain: str | None) -> str:
    value = str(grain or "").strip().lower()
    if value not in GRAINS:
        raise HealthScoreStoreError(f"grain must be one of {sorted(GRAINS)}, got {grain!r}")
    return value


def _checked_as_of(as_of: date | str | None) -> date:
    if as_of is None:
        return date.today()
    if isinstance(as_of, date):
        return as_of
    try:
        return date.fromisoformat(str(as_of).strip()[:10])
    except ValueError as exc:
        raise HealthScoreStoreError("as_of must be YYYY-MM-DD") from exc


def _row_exists(
    conn: sqlite3.Connection, entity_id: str, metric_name: str, grain: str, period_key: str
) -> bool:
    found = conn.execute(
        """
        SELECT 1 FROM healthscore_observation
        WHERE entity_id = ? AND metric_name = ? AND grain = ? AND period_key = ?
        """,
        (entity_id, metric_name, grain, period_key),
    ).fetchone()
    return found is not None


def _insert_shell(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    entity_name: str,
    metric_name: str,
    grain: str,
    period_key: str,
    captured_at: str,
    as_of: str,
) -> None:
    conn.execute(
        """
        INSERT INTO healthscore_observation (
            entity_id, entity_name, metric_name, grain, period_key,
            captured_at, as_of
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (entity_id, entity_name, metric_name, grain, period_key, captured_at, as_of),
    )


def upsert_reading(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    entity_name: str,
    metric_name: str,
    grain: str,
    as_of: date | str | None = None,
    period_key: str | None = None,
    value: float | None,
    points: float | None,
    generator: str | None,
    tags: list[str] | None = None,
    meta: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    """Write the generated reading, leaving any manual override in place."""
    checked_grain = _checked_grain(grain)
    day = _checked_as_of(as_of)
    key = period_key or period_key_for(checked_grain, day)
    captured_at = datetime.now(timezone.utc).isoformat()
    if not _row_exists(conn, entity_id, metric_name, checked_grain, key):
        _insert_shell(
            conn,
            entity_id=entity_id,
            entity_name=entity_name,
            metric_name=metric_name,
            grain=checked_grain,
            period_key=key,
            captured_at=captured_at,
            as_of=day.isoformat(),
        )
    conn.execute(
        """
        UPDATE healthscore_observation SET
            entity_name = ?, captured_at = ?, value = ?, points = ?,
            generator = ?, tags_json = ?, meta_json = ?, error = ?, as_of = ?
        WHERE entity_id = ? AND metric_name = ? AND grain = ? AND period_key = ?
        """,
        (
            entity_name,
            captured_at,
            value,
            points,
            generator,
            json.dumps(list(tags or [])),
            json.dumps(meta or {}, default=str),
            error,
            day.isoformat(),
            entity_id,
            metric_name,
            checked_grain,
            key,
        ),
    )
    conn.commit()
    return _fetch_one(conn, entity_id, metric_name, checked_grain, key)


def set_override(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    entity_name: str,
    metric_name: str,
    grain: str,
    as_of: date | str | None = None,
    period_key: str | None = None,
    value: float | None,
    points: float | None,
    by: str,
    note: str | None = None,
) -> dict[str, Any]:
    """Write a human reading that shadows — but never replaces — the generated one."""
    checked_grain = _checked_grain(grain)
    day = _checked_as_of(as_of)
    key = period_key or period_key_for(checked_grain, day)
    now = datetime.now(timezone.utc).isoformat()
    if not _row_exists(conn, entity_id, metric_name, checked_grain, key):
        _insert_shell(
            conn,
            entity_id=entity_id,
            entity_name=entity_name,
            metric_name=metric_name,
            grain=checked_grain,
            period_key=key,
            captured_at=now,
            as_of=day.isoformat(),
        )
    conn.execute(
        """
        UPDATE healthscore_observation SET
            entity_name = ?, override_value = ?, override_points = ?,
            override_note = ?, override_by = ?, override_at = ?
        WHERE entity_id = ? AND metric_name = ? AND grain = ? AND period_key = ?
        """,
        (
            entity_name,
            value,
            points,
            note,
            by,
            now,
            entity_id,
            metric_name,
            checked_grain,
            key,
        ),
    )
    conn.commit()
    return _fetch_one(conn, entity_id, metric_name, checked_grain, key)


def clear_override(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    metric_name: str,
    grain: str,
    period_key: str,
) -> None:
    conn.execute(
        """
        UPDATE healthscore_observation SET
            override_value = NULL, override_points = NULL, override_note = NULL,
            override_by = NULL, override_at = NULL
        WHERE entity_id = ? AND metric_name = ? AND grain = ? AND period_key = ?
        """,
        (entity_id, metric_name, _checked_grain(grain), period_key),
    )
    conn.commit()


def observations_for_entity(
    conn: sqlite3.Connection, entity_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM healthscore_observation
        WHERE entity_id = ?
        ORDER BY period_key DESC, metric_name ASC
        """,
        (entity_id,),
    ).fetchall()
    return [_row(row) for row in rows]


def latest_by_metric(
    observations: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """First row per metric; callers pass rows sorted newest period first."""
    out: dict[str, dict[str, Any]] = {}
    for row in observations:
        out.setdefault(str(row["metric_name"]), row)
    return out


def _fetch_one(
    conn: sqlite3.Connection, entity_id: str, metric_name: str, grain: str, period_key: str
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT * FROM healthscore_observation
        WHERE entity_id = ? AND metric_name = ? AND grain = ? AND period_key = ?
        """,
        (entity_id, metric_name, grain, period_key),
    ).fetchone()
    if row is None:
        raise HealthScoreStoreError(
            f"Health Score row missing after write: {entity_id}/{metric_name}/{period_key}"
        )
    return _row(row)


def _json_or(raw: Any, fallback: Any) -> Any:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return fallback


def _row(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    payload["tags"] = _json_or(payload.pop("tags_json", None), [])
    payload["meta"] = _json_or(payload.pop("meta_json", None), {})
    overridden = payload.get("override_value") is not None or payload.get("override_points") is not None
    payload["overridden"] = overridden
    # A points-only override still shows the generated value, and vice versa.
    payload["effective_value"] = (
        payload["override_value"]
        if payload.get("override_value") is not None
        else payload.get("value")
    )
    payload["effective_points"] = (
        payload["override_points"]
        if payload.get("override_points") is not None
        else payload.get("points")
    )
    payload["source_mode"] = "manual" if overridden else ("automated" if payload.get("generator") else "manual")
    payload["entered_by"] = payload.get("override_by") or payload.get("generator")
    payload["note"] = payload.get("override_note") or payload.get("error")
    return payload
