"""Engineering KPI deck: every active engineering metric, charts, notable moves.

No overall narrative. Each slide is one KPI. A front notable-changes list calls
out period-to-period successes and failures when the move is large enough to
matter. Help-desk (``support``-tagged) and pending SDD rows are out of scope.
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
    grain_for_generator,
    list_kpis,
)
from .metrics_digest import DigestRow, generate_digest_row
from .metrics_registry import (
    has_metric_generator,
    iter_metrics_with_generator,
    load_metrics_registry,
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
        val = row.observation.value
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


def build_eng_kpi_cards(
    *,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    history_conn: Any | None = None,
    as_of: date | None = None,
) -> list[EngKpiCard]:
    """Live digest rows plus stored history and notable-change flags."""
    ref = as_of or date.today()
    reg = registry if registry is not None else load_metrics_registry()
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
    for name, entry in iter_active_engineering_kpis(registry=reg):
        row = generate_digest_row(name, entry, registry=reg, ctx=resolve_ctx)
        gen = str(entry.get("metric-generator") or "").strip()
        grain = grain_for_generator(gen)
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


def build_engineering_kpi_slide_requests(
    cards: list[EngKpiCard],
    *,
    as_of: str,
    charts: Any | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    reqs: list[dict[str, Any]] = []
    sids: list[str] = []
    sids.append(append_title_slide(reqs, cards, as_of=as_of, slide_index=0))
    sids.append(append_notable_slide(reqs, cards, slide_index=1))
    for i, card in enumerate(cards):
        sids.append(append_kpi_slide(reqs, card, slide_index=i + 2, charts=charts))
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
    charts: Any | None = None,
) -> dict[str, Any]:
    """Create or update the persistent Engineering KPIs presentation."""
    as_of_s = as_of or date.today().isoformat()
    ref = date.fromisoformat(as_of_s[:10])
    if cards is None:
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
                registry=registry, ctx=ctx, history_conn=history_conn, as_of=ref
            )
        finally:
            if history_conn is not None:
                history_conn.close()

    from .deck_presentation_api import create_presentation
    from .drive_config import (
        _get_drive,
        dedupe_duplicate_names_in_folder,
        get_qbr_output_root_folder_id,
        list_files_by_name_in_folder,
        mirror_chart_spreadsheet_to_cortex,
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

    chart_helper = charts
    if chart_helper is None:
        try:
            from .charts import DeckCharts

            chart_helper = DeckCharts(PERSISTENT_TITLE)
        except Exception as e:
            logger.warning("Engineering KPI charts unavailable: %s", e)
            chart_helper = None

    reqs, sids = build_engineering_kpi_slide_requests(cards, as_of=as_of_s, charts=chart_helper)
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
    ss_id = getattr(chart_helper, "_ss_id", None) if chart_helper is not None else None
    if ss_id:
        mirror_chart_spreadsheet_to_cortex(ss_id, name=f"{PERSISTENT_TITLE} — Chart Data")

    result: dict[str, Any] = {
        "deck_id": deck_id,
        "deck_url": f"https://docs.google.com/presentation/d/{deck_id}/edit",
        "kpi_count": len(cards),
        "notable_count": sum(1 for c in cards if c.notable),
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
    ap.add_argument("-v", "--verbose", action="store_true")


def run_engineering_kpi_deck_cli(
    argv: Sequence[str] | None = None, *, prog: str = "engineering-kpis"
) -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Generate a Google Slides deck of active engineering KPIs (charts + notable changes).",
    )
    add_engineering_kpi_deck_arguments(ap)
    ns = ap.parse_args(list(argv) if argv is not None else None)
    if ns.verbose:
        logging.getLogger("cortex").setLevel(logging.INFO)

    print("Generating Engineering KPIs deck (no overall narrative)...")
    result = generate_engineering_kpi_deck(
        days=int(ns.days),
        timeout_seconds=float(ns.timeout),
        as_of=str(ns.date),
        db_path=Path(ns.db) if ns.db else None,
        skip_s3=bool(ns.skip_s3),
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
