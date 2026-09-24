"""Independent SQLite store for entity-level Health Score component readings."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS healthscore_observation (
    entity_id TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    component_key TEXT NOT NULL,
    period_date TEXT NOT NULL,
    raw_value_json TEXT,
    points REAL,
    source_mode TEXT NOT NULL,
    note TEXT,
    entered_by TEXT,
    entered_at TEXT NOT NULL,
    PRIMARY KEY (entity_id, component_key, period_date)
);
CREATE INDEX IF NOT EXISTS idx_healthscore_entity_period
ON healthscore_observation(entity_id, period_date DESC);
"""


def default_store_path() -> Path:
    from src.config import CORTEX_CACHE_ROOT

    return Path(CORTEX_CACHE_ROOT) / "healthscore" / "observations.sqlite"


def connect(path: Path | None = None) -> sqlite3.Connection:
    target = path or default_store_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def upsert_observation(
    conn: sqlite3.Connection,
    *,
    entity_id: str,
    entity_name: str,
    component_key: str,
    period_date: str | None,
    raw_value: Any,
    points: float | None,
    source_mode: str,
    note: str | None,
    entered_by: str,
) -> dict[str, Any]:
    period = (period_date or date.today().isoformat()).strip()
    try:
        date.fromisoformat(period)
    except ValueError as exc:
        raise ValueError("period_date must be YYYY-MM-DD") from exc
    mode = source_mode.strip().lower()
    if mode not in {"manual", "automated"}:
        raise ValueError("source_mode must be manual or automated")
    entered_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO healthscore_observation (
            entity_id, entity_name, component_key, period_date, raw_value_json,
            points, source_mode, note, entered_by, entered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(entity_id, component_key, period_date) DO UPDATE SET
            entity_name=excluded.entity_name,
            raw_value_json=excluded.raw_value_json,
            points=excluded.points,
            source_mode=excluded.source_mode,
            note=excluded.note,
            entered_by=excluded.entered_by,
            entered_at=excluded.entered_at
        """,
        (
            entity_id,
            entity_name,
            component_key,
            period,
            json.dumps(raw_value, default=str),
            points,
            mode,
            note,
            entered_by,
            entered_at,
        ),
    )
    conn.commit()
    return {
        "entity_id": entity_id,
        "entity_name": entity_name,
        "component_key": component_key,
        "period_date": period,
        "raw_value": raw_value,
        "points": points,
        "source_mode": mode,
        "note": note,
        "entered_by": entered_by,
        "entered_at": entered_at,
    }


def observations_for_entity(
    conn: sqlite3.Connection, entity_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM healthscore_observation
        WHERE entity_id = ?
        ORDER BY period_date DESC, component_key ASC
        """,
        (entity_id,),
    ).fetchall()
    return [_row(row) for row in rows]


def latest_by_component(
    observations: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in observations:
        out.setdefault(str(row["component_key"]), row)
    return out


def _row(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    try:
        payload["raw_value"] = json.loads(payload.pop("raw_value_json"))
    except (json.JSONDecodeError, TypeError):
        payload["raw_value"] = payload.pop("raw_value_json", None)
    return payload
