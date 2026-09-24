"""Four-page leadership KPI review for Engineering/DevOps and Support.

The deck is intentionally short: title, three largest moves, team narrative,
and a compact list of every active Engineering/DevOps KPI. Implementation is
omitted until its registry metrics have instrumented sources.
"""

from __future__ import annotations

import argparse
import logging
import sys
from calendar import month_abbr, month_name
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Sequence

from .kpi_snapshot import period_key_for
from .kpi_store import (
    GRAIN_MONTH,
    connect,
    default_kpi_store_path,
    list_kpis,
)
from .metrics_digest import DigestRow, generate_digest_row
from .metrics_registry import (
    has_metric_generator,
    iter_metrics_with_generator,
    load_metrics_registry,
    registry_metric_grain,
    registry_metric_tags,
)
from .metrics_upsert import MetricUpsertContext

logger = logging.getLogger("cortex")

PERSISTENT_TITLE = "Engineering KPIs"
REL_CHANGE_BAND = 0.10
PERCENT_POINT_BAND = 3.0
HISTORY_POINTS = 12
ERROR_DETAIL_MAX = 160

_KIND_SUCCESS = "success"
_KIND_FAILURE = "failure"
_KIND_WATCH = "watch"


@dataclass(frozen=True)
class HistoryPoint:
    period_key: str
    value: float


@dataclass(frozen=True)
class NotableChange:
    kind: str
    summary: str
    previous: float
    current: float


@dataclass(frozen=True)
class EngKpiCard:
    row: DigestRow
    grain: str
    history: tuple[HistoryPoint, ...]
    notable: NotableChange | None


def is_active_engineering_kpi(entry: Any) -> bool:
    """Instrumented engineering KPI: has a generator, not pending, not support-owned."""
    if not has_metric_generator(entry):
        return False
    tags = set(registry_metric_tags(entry))
    if "engineering" not in tags:
        return False
    if "pending" in tags:
        return False
    if "support" in tags:
        return False
    return True


def iter_active_engineering_kpis(
    *,
    registry: dict[str, Any] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    rows = [
        (name, entry)
        for name, entry in iter_metrics_with_generator(registry=registry)
        if is_active_engineering_kpi(entry)
    ]
    return sorted(rows, key=lambda item: item[0].lstrip(" %").casefold())


def is_active_support_kpi(entry: Any) -> bool:
    """Instrumented Support KPI, excluding unimplemented/pending rows."""
    if not has_metric_generator(entry):
        return False
    tags = set(registry_metric_tags(entry))
    return "support" in tags and "pending" not in tags


def iter_active_support_kpis(
    *,
    registry: dict[str, Any] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    rows = [
        (name, entry)
        for name, entry in iter_metrics_with_generator(registry=registry)
        if is_active_support_kpi(entry)
    ]
    return sorted(rows, key=lambda item: item[0].lstrip(" %").casefold())


def _period_label(period_key: str) -> str:
    key = (period_key or "").strip()
    if len(key) == 7 and key[4] == "-":
        year, month = int(key[:4]), int(key[5:7])
        return f"{month_abbr[month]} {year}"
    if len(key) >= 10:
        d = date.fromisoformat(key[:10])
        return f"{month_abbr[d.month]} {d.day}"
    return key


def classify_notable_change(
    *,
    previous: float | None,
    current: float | None,
    direction: str | None,
    target: float | None,
    unit: str | None,
    name: str,
    value_display: str,
    previous_display: str,
) -> NotableChange | None:
    """Flag a period-to-period move that is large or that crosses the target."""
    if previous is None or current is None:
        return None
    delta = current - previous
    if previous == 0:
        large = current != 0
        rel = None
    else:
        rel = delta / abs(previous)
        if unit == "percent":
            large = abs(delta) >= PERCENT_POINT_BAND or abs(rel) >= REL_CHANGE_BAND
        else:
            large = abs(rel) >= REL_CHANGE_BAND

    prev_on = _on_target(previous, target, direction)
    curr_on = _on_target(current, target, direction)
    crossed = prev_on is not None and curr_on is not None and prev_on != curr_on

    if not large and not crossed:
        return None

    better: bool | None = None
    if direction == "higher":
        better = current > previous
    elif direction == "lower":
        better = current < previous
    if crossed and curr_on is True:
        better = True
    elif crossed and curr_on is False:
        better = False

    if better is True:
        kind = _KIND_SUCCESS
    elif better is False:
        kind = _KIND_FAILURE
    else:
        kind = _KIND_WATCH

    if rel is None:
        move = "from zero" if previous == 0 else "changed"
    elif unit == "percent":
        move = f"{delta:+.1f} pts"
    else:
        move = f"{rel:+.0%}"
    bits = [f"{name} {previous_display} → {value_display} ({move})"]
    if crossed and curr_on is True:
        bits.append("now on target")
    elif crossed and curr_on is False:
        bits.append("now off target")
    return NotableChange(
        kind=kind,
        summary=" — ".join(bits),
        previous=previous,
        current=current,
    )


def error_callout(error: str) -> str:
    """Name the failing source on the slide instead of only ``error`` in the value."""
    detail = " ".join(str(error).split())
    if len(detail) > ERROR_DETAIL_MAX:
        detail = detail[: ERROR_DETAIL_MAX - 1].rstrip() + "…"
    return f"Could not compute: {detail}"


def _on_target(value: float, target: float | None, direction: str | None) -> bool | None:
    if target is None or not direction:
        return None
    if direction == "lower":
        return value <= target
    if direction == "higher":
        return value >= target
    return None


def history_points_for_metric(
    conn: Any,
    *,
    name: str,
    grain: str,
    limit: int = HISTORY_POINTS,
) -> list[HistoryPoint]:
    stored = list_kpis(conn, metric_name=name, grain=grain)
    points: list[HistoryPoint] = []
    for row in stored:
        val = row.effective_observation.display_value
        if val is None:
            continue
        points.append(HistoryPoint(period_key=row.period_key, value=float(val)))
    points = list(reversed(points[:limit]))
    return points


def merge_live_history(
    history: list[HistoryPoint],
    *,
    live_value: float | None,
    period_key: str,
) -> tuple[HistoryPoint, ...]:
    if live_value is None:
        return tuple(history)
    live = HistoryPoint(period_key=period_key, value=float(live_value))
    if history and history[-1].period_key == period_key:
        return tuple(history[:-1] + [live])
    return tuple(history + [live])


def _build_kpi_cards(
    entries: list[tuple[str, dict[str, Any]]],
    *,
    registry: dict[str, Any],
    ctx: MetricUpsertContext | None = None,
    history_conn: Any | None = None,
    as_of: date | None = None,
) -> list[EngKpiCard]:
    """Live rows plus stored history and notable-change flags for *entries*."""
    ref = as_of or date.today()
    resolve_ctx = ctx or MetricUpsertContext(
        entry_date=ref.isoformat(),
        requested_sites=None,
        skip_catalog=True,
        timeout_seconds=120.0,
        verbose=False,
        dry_run=True,
        days=30,
        max_issues_per_board=500,
        workers=6,
        metric_name_filter=None,
    )
    cards: list[EngKpiCard] = []
    for name, entry in entries:
        row = generate_digest_row(name, entry, registry=registry, ctx=resolve_ctx)
        grain = registry_metric_grain(entry)
        history: list[HistoryPoint] = []
        if history_conn is not None:
            history = history_points_for_metric(history_conn, name=name, grain=grain)
        period_key = period_key_for(grain, ref)
        merged = merge_live_history(history, live_value=row.value, period_key=period_key)
        previous = merged[-2].value if len(merged) >= 2 else None
        notable = classify_notable_change(
            previous=previous,
            current=row.value,
            direction=row.direction,
            target=row.target,
            unit=row.unit,
            name=row.name,
            value_display=row.value_display,
            previous_display=(
                DigestRow(
                    name=row.name,
                    metric_id=row.metric_id,
                    value=previous,
                    target=row.target,
                    direction=row.direction,
                    off_target=False,
                    unit=row.unit,
                ).value_display
                if previous is not None
                else "—"
            ),
        )
        cards.append(EngKpiCard(row=row, grain=grain, history=merged, notable=notable))
    return cards


def build_eng_kpi_cards(
    *,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    history_conn: Any | None = None,
    as_of: date | None = None,
) -> list[EngKpiCard]:
    """Build active Engineering/DevOps cards."""
    reg = registry if registry is not None else load_metrics_registry()
    return _build_kpi_cards(
        iter_active_engineering_kpis(registry=reg),
        registry=reg,
        ctx=ctx,
        history_conn=history_conn,
        as_of=as_of,
    )


def build_support_kpi_cards(
    *,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    history_conn: Any | None = None,
    as_of: date | None = None,
) -> list[EngKpiCard]:
    """Build active Support cards for summary ranking and team narrative."""
    reg = registry if registry is not None else load_metrics_registry()
    return _build_kpi_cards(
        iter_active_support_kpis(registry=reg),
        registry=reg,
        ctx=ctx,
        history_conn=history_conn,
        as_of=as_of,
    )


def open_history_store(*, db_path: Path | None = None, skip_s3: bool = False) -> Any | None:
    """Open the KPI SQLite store for history, or None when it is not present."""
    path = Path(db_path) if db_path else default_kpi_store_path()
    if not skip_s3:
        from .kpi_store_s3 import kpi_store_s3_uri, download_kpi_store, KPIStoreS3Error

        uri = kpi_store_s3_uri()
        if uri:
            import boto3

            try:
                download_kpi_store(path, s3_client=boto3.client("s3"), uri=uri)
            except KPIStoreS3Error as e:
                logger.warning("Engineering KPI history S3 download skipped: %s", e)
    if not path.is_file():
        logger.info("Engineering KPI deck: no stored history at %s", path)
        return None
    return connect(path)


def _header(reqs: list[dict[str, Any]], sid: str, title: str) -> None:
    from .slide_primitives import background, rect
    from .slide_requests import append_text_box
    from .slides_theme import CONTENT_W, FONT, MARGIN, NAVY, SLIDE_W, WHITE

    background(reqs, sid, WHITE)
    rect(reqs, f"{sid}_hdr", sid, 0, 0, SLIDE_W, 48, NAVY)
    append_text_box(reqs, f"{sid}_title", sid, MARGIN, 12, CONTENT_W, 28, title)
    reqs.append(
        {
            "updateTextStyle": {
                "objectId": f"{sid}_title",
                "style": {
                    "fontFamily": FONT,
                    "fontSize": {"magnitude": 20, "unit": "PT"},
                    "bold": True,
                    "foregroundColor": {"opaqueColor": {"rgbColor": WHITE}},
                },
                "textRange": {"type": "ALL"},
                "fields": "fontFamily,fontSize,bold,foregroundColor",
            }
        }
    )


def _style_body(reqs: list[dict[str, Any]], oid: str, *, size: float, color: dict, bold: bool = False) -> None:
    from .slides_theme import FONT

    reqs.append(
        {
            "updateTextStyle": {
                "objectId": oid,
                "style": {
                    "fontFamily": FONT,
                    "fontSize": {"magnitude": size, "unit": "PT"},
                    "bold": bold,
                    "foregroundColor": {"opaqueColor": {"rgbColor": color}},
                },
                "textRange": {"type": "ALL"},
                "fields": "fontFamily,fontSize,bold,foregroundColor",
            }
        }
    )


def append_title_slide(
    reqs: list[dict[str, Any]],
    cards: list[EngKpiCard],
    *,
    as_of: str,
    slide_index: int,
) -> str:
    from .slide_requests import append_slide, append_text_box
    from .slides_theme import BODY_Y, CONTENT_W, GRAY, MARGIN, NAVY

    sid = "eng_kpis_title"
    append_slide(reqs, sid, slide_index)
    _header(reqs, sid, PERSISTENT_TITLE)
    notable_n = sum(1 for c in cards if c.notable)
    month_close = sum(1 for c in cards if c.grain == GRAIN_MONTH)
    failed = [c for c in cards if c.row.error]
    lines = [
        f"{len(cards)} active engineering KPIs · as of {as_of}",
        f"{notable_n} notable period-to-period move(s) · {month_close} month-close, "
        f"{len(cards) - month_close} trailing-window",
        "No overall score. Each following slide is one KPI; charts use stored history when present.",
    ]
    y = BODY_Y + 12
    for i, line in enumerate(lines):
        oid = f"{sid}_l{i}"
        append_text_box(reqs, oid, sid, MARGIN, y, CONTENT_W, 22, line)
        _style_body(reqs, oid, size=14, color=NAVY if i == 0 else GRAY, bold=i == 0)
        y += 28
    if failed:
        oid = f"{sid}_err"
        names = ", ".join(sorted(c.row.name for c in failed))
        append_text_box(
            reqs,
            oid,
            sid,
            MARGIN,
            y,
            CONTENT_W,
            22,
            f"{len(failed)} KPI(s) could not be computed — {names}. Each slide names the failure.",
        )
        _style_body(reqs, oid, size=13, color={"red": 0.85, "green": 0.15, "blue": 0.15}, bold=True)
    return sid


def append_notable_slide(
    reqs: list[dict[str, Any]],
    cards: list[EngKpiCard],
    *,
    slide_index: int,
) -> str:
    from .slide_requests import append_slide, append_text_box
    from .slides_theme import BODY_Y, CONTENT_W, GRAY, MARGIN, NAVY

    sid = "eng_kpis_notable"
    append_slide(reqs, sid, slide_index)
    _header(reqs, sid, "Notable changes")
    notables = [c.notable for c in cards if c.notable]
    y = BODY_Y + 8
    if not notables:
        oid = f"{sid}_empty"
        append_text_box(
            reqs,
            oid,
            sid,
            MARGIN,
            y,
            CONTENT_W,
            22,
            "No period-to-period move above the 10% / 3-point band, and no target crossings.",
        )
        _style_body(reqs, oid, size=13, color=GRAY)
        return sid

    order = {_KIND_FAILURE: 0, _KIND_SUCCESS: 1, _KIND_WATCH: 2}
    notables = sorted(notables, key=lambda n: (order.get(n.kind, 9), n.summary.casefold()))
    worse = {"red": 0.85, "green": 0.15, "blue": 0.15}
    for i, item in enumerate(notables[:12]):
        color = worse if item.kind == _KIND_FAILURE else (NAVY if item.kind == _KIND_SUCCESS else GRAY)
        label = {"success": "Better", "failure": "Worse", "watch": "Move"}[item.kind]
        line = f"{label}  {item.summary}"
        oid = f"{sid}_n{i}"
        append_text_box(reqs, oid, sid, MARGIN, y, CONTENT_W, 18, line)
        _style_body(reqs, oid, size=12, color=color, bold=item.kind != _KIND_WATCH)
        y += 22
    if len(notables) > 12:
        oid = f"{sid}_more"
        append_text_box(
            reqs, oid, sid, MARGIN, y, CONTENT_W, 16, f"+ {len(notables) - 12} more on the KPI slides"
        )
        _style_body(reqs, oid, size=11, color=GRAY)
    return sid


def append_kpi_slide(
    reqs: list[dict[str, Any]],
    card: EngKpiCard,
    *,
    slide_index: int,
    charts: Any | None,
) -> str:
    from .charts import embed_chart
    from .slide_primitives import kpi_metric_card
    from .slide_requests import append_slide, append_text_box
    from .slides_theme import (
        BLUE,
        BODY_Y,
        CONTENT_W,
        GRAY,
        MARGIN,
        NAVY,
        SLIDE_H,
    )

    sid = f"eng_kpi_{slide_index}"
    append_slide(reqs, sid, slide_index)
    _header(reqs, sid, card.row.name)

    worse = {"red": 0.85, "green": 0.15, "blue": 0.15}
    delta_label = "—"
    delta_accent = NAVY
    if card.notable:
        delta_label = {"success": "Better", "failure": "Worse", "watch": "Moved"}[card.notable.kind]
        delta_accent = worse if card.notable.kind == _KIND_FAILURE else BLUE
    elif len(card.history) >= 2:
        delta_label = "Flat"

    kpi_y = BODY_Y + 6
    kpi_h = 52
    gap = 10
    col_w = (CONTENT_W - 2 * gap) / 3
    kpi_metric_card(
        reqs, f"{sid}_k0", sid, MARGIN, kpi_y, col_w, kpi_h, "Current", card.row.value_display, accent=NAVY, value_pt=20
    )
    kpi_metric_card(
        reqs,
        f"{sid}_k1",
        sid,
        MARGIN + col_w + gap,
        kpi_y,
        col_w,
        kpi_h,
        "Target",
        card.row.target_display,
        accent=BLUE,
        value_pt=20,
    )
    kpi_metric_card(
        reqs,
        f"{sid}_k2",
        sid,
        MARGIN + 2 * (col_w + gap),
        kpi_y,
        col_w,
        kpi_h,
        "Vs prior period",
        delta_label,
        accent=delta_accent,
        value_pt=18,
    )

    callout_y = kpi_y + kpi_h + 10
    if card.row.error:
        oid = f"{sid}_err"
        append_text_box(
            reqs, oid, sid, MARGIN, callout_y, CONTENT_W, 18, error_callout(card.row.error)
        )
        _style_body(reqs, oid, size=12, color=worse, bold=True)
        chart_top = callout_y + 22
    elif card.notable:
        oid = f"{sid}_call"
        append_text_box(reqs, oid, sid, MARGIN, callout_y, CONTENT_W, 18, card.notable.summary)
        _style_body(reqs, oid, size=12, color=delta_accent, bold=True)
        chart_top = callout_y + 22
    else:
        chart_top = callout_y

    footer_y = SLIDE_H - 10 - 18
    content_bottom = footer_y - 8
    grain_label = "month-close" if card.grain == GRAIN_MONTH else "trailing window"
    footer = f"{grain_label} · stored history when present · better when {card.row.direction or 'unset'}"
    append_text_box(reqs, f"{sid}_ft", sid, MARGIN, footer_y, CONTENT_W, 16, footer)
    _style_body(reqs, f"{sid}_ft", size=9, color=GRAY)

    labels = [_period_label(p.period_key) for p in card.history]
    values = [p.value for p in card.history]
    if charts is not None and len(values) >= 2:
        ss_id, chart_id = charts.add_line_chart(
            title="",
            labels=labels,
            series={"Value": values},
            show_legend=False,
        )
        embed_chart(
            reqs,
            f"{sid}_chart",
            sid,
            ss_id,
            chart_id,
            MARGIN,
            chart_top,
            CONTENT_W,
            max(80.0, content_bottom - chart_top),
            linked=False,
        )
    else:
        oid = f"{sid}_nohist"
        msg = (
            "Not enough stored history for a trend (need two periods)."
            if len(values) < 2
            else "Chart omitted."
        )
        append_text_box(reqs, oid, sid, MARGIN, chart_top + 20, CONTENT_W, 20, msg)
        _style_body(reqs, oid, size=12, color=GRAY)
    return sid


def movement_score(card: EngKpiCard) -> float:
    """Comparable magnitude for ranking changes across unlike KPI units."""
    if not card.notable:
        return -1.0
    previous = card.notable.previous
    delta = abs(card.notable.current - previous)
    return delta / abs(previous) if previous else delta


def top_changed_cards(
    engineering_cards: list[EngKpiCard],
    support_cards: list[EngKpiCard],
    *,
    limit: int = 3,
) -> list[EngKpiCard]:
    """Return at most *limit* largest period-over-period changes."""
    changed = [c for c in engineering_cards + support_cards if c.notable]
    return sorted(changed, key=lambda c: (-movement_score(c), c.row.name.casefold()))[:limit]


def _movement_label(card: EngKpiCard) -> str:
    if len(card.history) < 2 or card.row.value is None:
        return "—"
    previous = card.history[-2].value
    current = float(card.row.value)
    if current > previous:
        return "↑"
    if current < previous:
        return "↓"
    return "→"


def append_deck_title_slide(
    reqs: list[dict[str, Any]], *, as_of: str, slide_index: int = 0
) -> str:
    """A true title page: title, scope, and date only."""
    from .slide_primitives import background, rect
    from .slide_requests import append_slide, append_text_box
    from .slides_theme import BLUE, CONTENT_W, GRAY, MARGIN, NAVY, SLIDE_H, SLIDE_W, WHITE

    sid = "eng_kpis_title"
    append_slide(reqs, sid, slide_index)
    background(reqs, sid, NAVY)
    rect(reqs, f"{sid}_band", sid, 0, 0, 18, SLIDE_H, BLUE)
    append_text_box(reqs, f"{sid}_title", sid, MARGIN + 18, 128, CONTENT_W - 18, 54, PERSISTENT_TITLE)
    _style_body(reqs, f"{sid}_title", size=30, color=WHITE, bold=True)
    append_text_box(
        reqs,
        f"{sid}_scope",
        sid,
        MARGIN + 18,
        196,
        CONTENT_W - 18,
        28,
        "Engineering/DevOps & Support",
    )
    _style_body(
        reqs,
        f"{sid}_scope",
        size=17,
        color={"red": 0.72, "green": 0.82, "blue": 0.9},
    )
    append_text_box(
        reqs, f"{sid}_date", sid, MARGIN + 18, 244, CONTENT_W - 18, 22, f"As of {as_of}"
    )
    _style_body(
        reqs,
        f"{sid}_date",
        size=12,
        color={"red": 0.72, "green": 0.82, "blue": 0.9},
    )
    rect(reqs, f"{sid}_rule", sid, MARGIN + 18, 286, SLIDE_W - MARGIN * 2 - 18, 3, BLUE)
    return sid


def append_summary_slide(
    reqs: list[dict[str, Any]],
    changed: list[EngKpiCard],
    *,
    slide_index: int,
) -> str:
    """Deterministic summary fallback with no more than three changed KPIs."""
    from .slide_primitives import rect
    from .slide_requests import append_slide, append_text_box
    from .slides_theme import BLUE, CONTENT_W, GRAY, MARGIN, NAVY

    sid = "eng_kpis_summary"
    append_slide(reqs, sid, slide_index)
    _header(reqs, sid, "Summary — Largest Changes")
    if not changed:
        append_text_box(
            reqs, f"{sid}_empty", sid, MARGIN, 92, CONTENT_W, 28, "No material period-over-period changes."
        )
        _style_body(reqs, f"{sid}_empty", size=15, color=GRAY)
        return sid
    gap = 12.0
    w = (CONTENT_W - gap * 2) / 3
    for i, card in enumerate(changed[:3]):
        x = MARGIN + i * (w + gap)
        fill = {"red": 0.99, "green": 0.93, "blue": 0.92}
        accent = {"red": 0.76, "green": 0.22, "blue": 0.17}
        if card.notable and card.notable.kind == _KIND_SUCCESS:
            fill = {"red": 0.91, "green": 0.97, "blue": 0.99}
            accent = BLUE
        rect(reqs, f"{sid}_bg{i}", sid, x, 86, w, 220, fill)
        append_text_box(reqs, f"{sid}_n{i}", sid, x + 12, 102, w - 24, 38, card.row.name)
        _style_body(reqs, f"{sid}_n{i}", size=14, color=NAVY, bold=True)
        append_text_box(
            reqs, f"{sid}_v{i}", sid, x + 12, 154, w - 24, 40, card.row.value_display
        )
        _style_body(reqs, f"{sid}_v{i}", size=24, color=accent, bold=True)
        detail = card.notable.summary if card.notable else "No material change"
        append_text_box(reqs, f"{sid}_d{i}", sid, x + 12, 212, w - 24, 74, detail)
        _style_body(reqs, f"{sid}_d{i}", size=10, color=NAVY)
    return sid


def _team_status_line(cards: list[EngKpiCard]) -> str:
    measured = [c for c in cards if c.row.value is not None and not c.row.error and c.row.target is not None]
    off = [c for c in measured if c.row.off_target]
    unavailable = [c for c in cards if c.row.value is None or c.row.error]
    return (
        f"{len(measured) - len(off)} on target · {len(off)} off target"
        + (f" · {len(unavailable)} unavailable" if unavailable else "")
    )


def append_team_narrative_slide(
    reqs: list[dict[str, Any]],
    engineering_cards: list[EngKpiCard],
    support_cards: list[EngKpiCard],
    *,
    slide_index: int,
) -> str:
    """Factual fallback when Claude is explicitly disabled."""
    from .slide_requests import append_slide, append_text_box
    from .slides_theme import CONTENT_W, GRAY, MARGIN, NAVY

    sid = "eng_kpis_teams"
    append_slide(reqs, sid, slide_index)
    _header(reqs, sid, "How Each Team Is Doing")
    sections = [
        ("Engineering/DevOps", engineering_cards),
        ("Support", support_cards),
    ]
    y = 80.0
    for i, (name, cards) in enumerate(sections):
        append_text_box(reqs, f"{sid}_h{i}", sid, MARGIN, y, CONTENT_W, 24, name)
        _style_body(reqs, f"{sid}_h{i}", size=17, color=NAVY, bold=True)
        y += 30
        append_text_box(
            reqs, f"{sid}_s{i}", sid, MARGIN, y, CONTENT_W, 22, _team_status_line(cards)
        )
        _style_body(reqs, f"{sid}_s{i}", size=13, color=GRAY)
        y += 64
    return sid


def _short_definition(card: EngKpiCard, *, limit: int = 94) -> str:
    text = " ".join(str(card.row.description or "No definition provided.").split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _compact_value_display(card: EngKpiCard) -> str:
    """Keep large counts on one line in the two-column KPI list."""
    if card.row.value is None or card.row.error:
        return card.row.value_display
    value = float(card.row.value)
    magnitude = abs(value)
    if magnitude >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if magnitude >= 10_000:
        return f"{value / 1_000:.1f}K"
    return card.row.value_display


def append_kpi_list_slide(
    reqs: list[dict[str, Any]],
    cards: list[EngKpiCard],
    *,
    slide_index: int,
) -> str:
    """Page 4: readable two-column list with every requested KPI field."""
    from .slide_primitives import rect
    from .slide_requests import append_slide, append_text_box
    from .slides_theme import BLUE, CONTENT_W, GRAY, MARGIN, NAVY

    sid = "eng_kpis_list"
    append_slide(reqs, sid, slide_index)
    measured = [c for c in cards if c.row.target is not None and not c.row.error]
    off = sum(1 for c in measured if c.row.off_target)
    title = (
        f"{off} of {len(measured)} Targeted KPIs Are Off Track"
        if measured
        else "Engineering/DevOps KPI List"
    )
    _header(reqs, sid, title)
    append_text_box(
        reqs,
        f"{sid}_legend",
        sid,
        MARGIN,
        56,
        CONTENT_W,
        15,
        "Name  ·  Value  ·  Up/Down  ·  Target  ·  Definition",
    )
    _style_body(reqs, f"{sid}_legend", size=8.5, color=GRAY, bold=True)

    gap = 18.0
    col_w = (CONTENT_W - gap) / 2
    per_col = (len(cards) + 1) // 2
    row_h = min(34.0, 310.0 / max(1, per_col))
    for r, card in enumerate(cards):
        col = r // per_col
        row = r % per_col
        x = MARGIN + col * (col_w + gap)
        y = 76 + row * row_h
        if row % 2:
            rect(
                reqs,
                f"{sid}_bg{r}",
                sid,
                x,
                y - 1,
                col_w,
                row_h,
                {"red": 0.96, "green": 0.97, "blue": 0.98},
            )
        fields = [
            (card.row.name, 0.0, 160.0, True),
            (_compact_value_display(card), 160.0, 48.0, False),
            (_movement_label(card), 208.0, 18.0, False),
            (f"T: {card.row.target_display}", 226.0, col_w - 231.0, False),
        ]
        for c, (value, dx, width, bold) in enumerate(fields):
            oid = f"{sid}_p{r}c{c}"
            append_text_box(reqs, oid, sid, x + 5 + dx, y + 2, width, 13, value)
            _style_body(reqs, oid, size=8.2, color=NAVY, bold=bold)
        append_text_box(
            reqs,
            f"{sid}_d{r}",
            sid,
            x + 5,
            y + 16,
            col_w - 10,
            max(11.0, row_h - 17),
            _short_definition(card, limit=72),
        )
        _style_body(reqs, f"{sid}_d{r}", size=8.0, color=GRAY)
        rect(reqs, f"{sid}_rule{r}", sid, x, y + row_h - 1, col_w, 0.6, BLUE)
    return sid


def append_middle_slides(
    reqs: list[dict[str, Any]],
    engineering_cards: list[EngKpiCard],
    support_cards: list[EngKpiCard],
    *,
    as_of: str,
    use_claude: bool | None = None,
) -> tuple[int, list[str]]:
    """Pages 2–3, designed by Claude when enabled."""
    from .eng_kpi_claude_slides import (
        EngKpiClaudeError,
        eng_kpi_claude_allow_fallback,
        eng_kpi_claude_enabled,
        render_eng_kpi_claude_slides,
    )

    claude_on = eng_kpi_claude_enabled() if use_claude is None else bool(use_claude)
    if claude_on:
        try:
            return render_eng_kpi_claude_slides(
                reqs,
                engineering_cards,
                support_cards,
                as_of=as_of,
                start_index=1,
            )
        except EngKpiClaudeError as e:
            if not eng_kpi_claude_allow_fallback():
                raise
            logger.warning(
                "CORTEX_METRICS_CLAUDE_ALLOW_FALLBACK: using hand-built "
                "summary and team narrative slides (%s)",
                e,
            )

    changed = top_changed_cards(engineering_cards, support_cards)
    sids = [
        append_summary_slide(reqs, changed, slide_index=1),
        append_team_narrative_slide(
            reqs, engineering_cards, support_cards, slide_index=2
        ),
    ]
    return 3, sids


def build_engineering_kpi_slide_requests(
    cards: list[EngKpiCard],
    *,
    as_of: str,
    support_cards: list[EngKpiCard] | None = None,
    charts: Any | None = None,
    use_claude: bool | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    reqs: list[dict[str, Any]] = []
    support = support_cards or []
    sids = [append_deck_title_slide(reqs, as_of=as_of, slide_index=0)]
    next_index, middle_sids = append_middle_slides(
        reqs, cards, support, as_of=as_of, use_claude=use_claude
    )
    sids.extend(middle_sids)
    sids.append(append_kpi_list_slide(reqs, cards, slide_index=next_index))
    return reqs, sids


def generate_engineering_kpi_deck(
    *,
    days: int = 30,
    timeout_seconds: float = 120.0,
    as_of: str | None = None,
    registry: dict[str, Any] | None = None,
    db_path: Path | None = None,
    skip_s3: bool = False,
    cards: list[EngKpiCard] | None = None,
    support_cards: list[EngKpiCard] | None = None,
    charts: Any | None = None,
    use_claude: bool | None = None,
) -> dict[str, Any]:
    """Create or update the persistent Engineering KPIs presentation."""
    as_of_s = as_of or date.today().isoformat()
    ref = date.fromisoformat(as_of_s[:10])
    if cards is None:
        reg = registry if registry is not None else load_metrics_registry()
        ctx = MetricUpsertContext(
            entry_date=as_of_s,
            requested_sites=None,
            skip_catalog=True,
            timeout_seconds=timeout_seconds,
            verbose=False,
            dry_run=True,
            days=days,
            max_issues_per_board=500,
            workers=6,
            metric_name_filter=None,
        )
        history_conn = open_history_store(db_path=db_path, skip_s3=skip_s3)
        try:
            cards = build_eng_kpi_cards(
                registry=reg, ctx=ctx, history_conn=history_conn, as_of=ref
            )
            support_cards = build_support_kpi_cards(
                registry=reg, ctx=ctx, history_conn=history_conn, as_of=ref
            )
        finally:
            if history_conn is not None:
                history_conn.close()
    support_cards = support_cards or []

    from .deck_presentation_api import create_presentation
    from .drive_config import (
        _get_drive,
        dedupe_duplicate_names_in_folder,
        get_qbr_output_root_folder_id,
        list_files_by_name_in_folder,
        mirror_finished_drive_file,
    )
    from .metrics_digest import _copy_metrics_presentation_to_historical
    from .slides_api import _get_service

    output_folder = get_qbr_output_root_folder_id()
    if not output_folder:
        return {"error": "No Output folder configured (set GOOGLE_QBR_GENERATOR_FOLDER_ID)"}

    try:
        drive_svc = _get_drive()
        existing = list_files_by_name_in_folder(
            PERSISTENT_TITLE,
            output_folder,
            mime_type="application/vnd.google-apps.presentation",
        )
        if existing:
            deck_id = str(existing[0]["id"])
            dedupe_duplicate_names_in_folder(output_folder, PERSISTENT_TITLE)
            logger.info("Reusing persistent presentation %s: %s", deck_id, PERSISTENT_TITLE)
        else:
            deck_id, err = create_presentation(drive_svc, PERSISTENT_TITLE, output_folder_id=output_folder)
            if err or not deck_id:
                return {"error": f"Failed to create presentation: {err}"}
    except Exception as e:
        return {"error": f"Failed to create/find presentation: {e}"}

    slides_svc, _, _ = _get_service()
    try:
        pres = slides_svc.presentations().get(presentationId=deck_id).execute()
        existing_slides = pres.get("slides") or []
        if existing_slides:
            delete_reqs = [{"deleteObject": {"objectId": s["objectId"]}} for s in existing_slides]
            slides_svc.presentations().batchUpdate(
                presentationId=deck_id, body={"requests": delete_reqs}
            ).execute()
    except Exception as e:
        logger.warning("Could not clear existing Engineering KPIs slides: %s", e)

    from .eng_kpi_claude_slides import EngKpiClaudeError

    try:
        reqs, sids = build_engineering_kpi_slide_requests(
            cards,
            support_cards=support_cards,
            as_of=as_of_s,
            charts=charts,
            use_claude=use_claude,
        )
    except EngKpiClaudeError as e:
        return {"error": f"Claude Engineering KPI slides failed: {e}"}
    slides_svc.presentations().batchUpdate(presentationId=deck_id, body={"requests": reqs}).execute()
    try:
        pres = slides_svc.presentations().get(presentationId=deck_id).execute()
        extra = [
            {"deleteObject": {"objectId": slide["objectId"]}}
            for slide in (pres.get("slides") or [])
            if slide.get("objectId") and slide["objectId"] not in set(sids)
        ]
        if extra:
            slides_svc.presentations().batchUpdate(
                presentationId=deck_id, body={"requests": extra}
            ).execute()
    except Exception as e:
        logger.warning("Could not clean leftover Engineering KPIs slides: %s", e)

    month = date.fromisoformat(as_of_s[:10])
    historical_title = f"{PERSISTENT_TITLE} - {month_name[month.month]}"
    historical_url = _copy_metrics_presentation_to_historical(
        drive_svc=drive_svc,
        deck_id=deck_id,
        historical_title=historical_title,
        tag_label="ENGINEERING KPIS",
        as_of_s=as_of_s,
    )
    mirror_finished_drive_file(deck_id, name=PERSISTENT_TITLE, source_parent_id=output_folder)

    result: dict[str, Any] = {
        "deck_id": deck_id,
        "deck_url": f"https://docs.google.com/presentation/d/{deck_id}/edit",
        "kpi_count": len(cards),
        "support_kpi_count": len(support_cards),
        "notable_count": len(top_changed_cards(cards, support_cards)),
    }
    if historical_url:
        result["historical_url"] = historical_url
    return result


def add_engineering_kpi_deck_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--days", type=int, default=30, help="Trailing window for generators (default: 30)")
    ap.add_argument("--timeout", type=float, default=120.0, metavar="SEC")
    ap.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Report as-of date (default: today)",
    )
    ap.add_argument("--db", default=None, help="KPI SQLite path (default: cache store)")
    ap.add_argument("--skip-s3", action="store_true", help="Do not download the KPI store from S3")
    ap.add_argument(
        "--claude",
        dest="use_claude",
        action="store_true",
        default=None,
        help="Have Claude design the summary and team narrative (default when ANTHROPIC_API_KEY is set)",
    )
    ap.add_argument(
        "--no-claude",
        dest="use_claude",
        action="store_false",
        help="Use factual fixed-layout summary and team slides",
    )
    ap.add_argument("-v", "--verbose", action="store_true")


def run_engineering_kpi_deck_cli(
    argv: Sequence[str] | None = None, *, prog: str = "engineering-kpis"
) -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Generate the four-page Engineering/DevOps and Support KPI review.",
    )
    add_engineering_kpi_deck_arguments(ap)
    ns = ap.parse_args(list(argv) if argv is not None else None)
    if ns.verbose:
        logging.getLogger("cortex").setLevel(logging.INFO)

    from .eng_kpi_claude_slides import eng_kpi_claude_enabled

    claude_on = eng_kpi_claude_enabled() if ns.use_claude is None else bool(ns.use_claude)
    designer = "Claude summary + team narrative" if claude_on else "fixed summary + team status"
    print(f"Generating four-page Engineering KPIs deck ({designer})...")
    result = generate_engineering_kpi_deck(
        days=int(ns.days),
        timeout_seconds=float(ns.timeout),
        as_of=str(ns.date),
        db_path=Path(ns.db) if ns.db else None,
        skip_s3=bool(ns.skip_s3),
        use_claude=ns.use_claude,
    )
    if result.get("error"):
        print(f"Error: {result['error']}", file=sys.stderr)
        return 1
    print()
    print("=" * 60)
    print(f"  OK   {result['deck_url']}")
    if result.get("historical_url"):
        print(f"  Hist {result['historical_url']}")
    print("=" * 60)
    return 0
