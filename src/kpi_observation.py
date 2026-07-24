"""Canonical KPI observation returned by generators and persistence adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class KPIObservation:
    """One computed or stored KPI value with provenance.

    Generators and stores both produce this shape so decks, CLIs, and LeanDNA
    materialization share one contract. Persistence is optional — ``origin``
    records where the value came from.
    """

    value: Any = None
    numerator: float | None = None
    denominator: float | None = None
    as_of: str | None = None
    window_days: int | None = None
    source: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    error: str | None = None
    origin: str = "live"  # live | stored | none
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.error is None and self.display_value is not None

    @property
    def display_value(self) -> Any:
        """Prefer explicit ``value``; else ratio from numerator/denominator."""
        if self.error is not None:
            return None
        if self.value is not None:
            return self.value
        if self.numerator is not None and self.denominator is not None:
            if float(self.denominator) == 0.0:
                return None
            return float(self.numerator) / float(self.denominator)
        return None


def utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def observation_from_generator_raw(
    raw: Any,
    *,
    metric_name: str,
    origin: str = "live",
) -> KPIObservation:
    """Normalize a generator return value into a ``KPIObservation``.

    Accepts the same shapes ``metrics_upsert.parse_generator_parts`` understands,
    plus an already-built ``KPIObservation``.
    """
    if isinstance(raw, KPIObservation):
        if raw.origin == origin:
            return raw
        return KPIObservation(
            value=raw.value,
            numerator=raw.numerator,
            denominator=raw.denominator,
            as_of=raw.as_of or utc_today_iso(),
            window_days=raw.window_days,
            source=raw.source,
            warnings=raw.warnings,
            error=raw.error,
            origin=origin,
            meta=dict(raw.meta),
        )

    if raw is None:
        return KPIObservation(error="generator returned None", origin=origin, as_of=utc_today_iso())

    if isinstance(raw, dict):
        err = raw.get("error")
        if err:
            return KPIObservation(error=str(err), origin=origin, as_of=utc_today_iso())

        window = raw.get("window_days")
        window_i = int(window) if window is not None else None
        sources: tuple[str, ...] = ()
        if isinstance(raw.get("source"), (list, tuple)):
            sources = tuple(str(s) for s in raw["source"])
        elif isinstance(raw.get("source"), str) and raw["source"].strip():
            sources = (raw["source"].strip(),)

        warnings: tuple[str, ...] = ()
        if isinstance(raw.get("warnings"), (list, tuple)):
            warnings = tuple(str(w) for w in raw["warnings"])

        meta = {
            k: v
            for k, v in raw.items()
            if k
            not in {
                "value",
                "numerator",
                "denominator",
                "error",
                "window_days",
                "source",
                "warnings",
                "as_of",
                "teams",
                "mode",
            }
        }

        if "numerator" in raw and "denominator" in raw:
            num = float(raw["numerator"])
            den = float(raw["denominator"])
            value = raw.get("value")
            # Percent KPIs are often named "% WAU" (leading %) or "KPI Automation %".
            is_pct = "%" in str(metric_name)
            if value is None and den != 0 and is_pct:
                value = round(100.0 * num / den, 2)
            elif value is None and den != 0:
                value = num / den
            return KPIObservation(
                value=value,
                numerator=num,
                denominator=den,
                as_of=str(raw.get("as_of") or utc_today_iso())[:10],
                window_days=window_i,
                source=sources,
                warnings=warnings,
                origin=origin,
                meta=meta,
            )

        if "value" in raw and raw.get("value") is not None:
            return KPIObservation(
                value=raw["value"],
                numerator=float(raw["numerator"]) if raw.get("numerator") is not None else None,
                denominator=float(raw["denominator"]) if raw.get("denominator") is not None else 1.0,
                as_of=str(raw.get("as_of") or utc_today_iso())[:10],
                window_days=window_i,
                source=sources,
                warnings=warnings,
                origin=origin,
                meta=meta,
            )

        # Cycle-time team payload — median of team medians.
        if "teams" in raw or raw.get("mode") == "history":
            from statistics import median

            medians: list[float] = []
            for team in raw.get("teams") or []:
                if not isinstance(team, dict) or team.get("error"):
                    continue
                raw_med = team.get("median_days")
                if raw_med is None:
                    overall = team.get("overall") or {}
                    raw_med = overall.get("median_days")
                if raw_med is not None:
                    medians.append(float(raw_med))
            if not medians:
                return KPIObservation(
                    error="no median_days from development cycle time teams",
                    origin=origin,
                    as_of=utc_today_iso(),
                )
            med = float(median(medians))
            return KPIObservation(
                value=med,
                numerator=med,
                denominator=1.0,
                as_of=utc_today_iso(),
                window_days=window_i,
                origin=origin,
                meta=meta,
            )

        return KPIObservation(
            error=f"unsupported generator result keys: {sorted(raw.keys())[:8]}",
            origin=origin,
            as_of=utc_today_iso(),
        )

    if isinstance(raw, (int, float)):
        return KPIObservation(
            value=raw,
            numerator=float(raw),
            denominator=1.0,
            as_of=utc_today_iso(),
            origin=origin,
        )

    return KPIObservation(
        error=f"unsupported generator result type: {type(raw).__name__}",
        origin=origin,
        as_of=utc_today_iso(),
    )


def observation_from_stored_datapoint(
    *,
    date_s: str | None,
    value: Any,
    metric_id: int | None = None,
) -> KPIObservation:
    if date_s is None and value is None:
        return KPIObservation(origin="none", error=None, as_of=None)
    return KPIObservation(
        value=value,
        numerator=float(value) if isinstance(value, (int, float)) else None,
        denominator=1.0 if isinstance(value, (int, float)) else None,
        as_of=(date_s or utc_today_iso())[:10],
        origin="stored",
        source=("data-api",),
        meta={"metric_id": metric_id} if metric_id else {},
    )
