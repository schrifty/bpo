"""Morning report: overnight ECS job status + live KPI digest emailed via SES.

Opens with last night's scheduled job outcomes (CloudWatch ``CORTEX_RUN_SUMMARY``),
then off-target metrics (vs ``target`` / ``direction`` in ``config/my-metrics.yaml``),
then remaining metrics alphabetically.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from src.metrics_registry import (
    entry_has_tag,
    digest_display_unit,
    has_metric_id,
    iter_metrics_with_generator,
    load_metrics_registry,
    normalize_tag,
    registry_metric_description,
    registry_metric_direction,
    registry_metric_tags,
    registry_metric_target,
    validate_metric_target_direction,
)
from src.metrics_upsert import (
    MetricParts,
    MetricUpsertContext,
    MetricUpsertError,
    invoke_metric_generator,
    parse_generator_parts,
)
from src.overnight_jobs_report import (
    OvernightJobOutcome,
    collect_overnight_job_outcomes,
    format_overnight_jobs_section,
    overnight_failure_count,
)
from src.ses_email import SesEmailError, digest_email_from, digest_email_recipients, send_email

logger = logging.getLogger("cortex")

# Soft wrap / detail budget. Primary table columns use compact natural widths
# only — never pad to fill a target width.
DIGEST_LINE_WIDTH = 128
_DIGEST_COL_GAP = 2
_DIGEST_COL_COUNT = 7
# Inline VALUE column cap — longer values / errors go on a following detail line.
_DIGEST_VALUE_INLINE_MAX = 24
# Max characters for error / overflow detail lines under a row.
_DIGEST_DETAIL_MAX = 132
# Registry description column: +40% truncation budget vs error detail lines.
_DIGEST_DESCRIPTION_MAX = int(round(_DIGEST_DETAIL_MAX * 1.4))  # 185


def _truncate_digest_detail(text: str, *, max_len: int = _DIGEST_DETAIL_MAX) -> str:
    """Single-line detail: collapse whitespace and truncate to *max_len* with ellipsis."""
    collapsed = " ".join(str(text).split())
    if max_len <= 0:
        return ""
    if len(collapsed) <= max_len:
        return collapsed
    if max_len == 1:
        return "…"
    return collapsed[: max_len - 1] + "…"


def _detail_lines(
    text: str,
    *,
    prefix: str = "  ",
    max_len: int = _DIGEST_DETAIL_MAX,
) -> list[str]:
    """One prefixed detail line, truncated to *max_len*."""
    return [f"{prefix}{_truncate_digest_detail(text, max_len=max_len)}"]



def _format_plain_number(value: float) -> str:
    """Format a number with thousands separators; trim trailing decimal zeros."""
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def _format_digest_number(value: float, *, unit: str | None = None) -> str:
    """Format a digest VALUE/TARGET cell (currency, percent, or plain with commas)."""
    if unit == "currency":
        if float(value).is_integer():
            return f"${int(value):,}"
        return f"${value:,.2f}"
    if unit == "percent":
        return f"{_format_plain_number(value)}%"
    return _format_plain_number(value)


@dataclass(frozen=True)
class DigestRow:
    name: str
    metric_id: int | None
    value: float | None
    target: float | None
    direction: str | None
    off_target: bool
    error: str | None = None
    description: str | None = None
    tags: tuple[str, ...] = ()
    unit: str | None = None
    context: str | None = None  # e.g. "$9,950 / 288 issues"

    @property
    def id_display(self) -> str:
        return str(self.metric_id) if self.metric_id is not None else "—"

    @property
    def value_display(self) -> str:
        if self.error:
            return "error"
        if self.value is None:
            return "—"
        return _format_digest_number(float(self.value), unit=self.unit)

    @property
    def target_display(self) -> str:
        if self.target is None:
            return "—"
        return _format_digest_number(float(self.target), unit=self.unit)

    @property
    def tags_display(self) -> str:
        return ", ".join(self.tags) if self.tags else "—"


def _build_context_string(name: str, raw: dict[str, Any], unit: str | None) -> str | None:
    """Build a short context string from generator output (e.g. '48 / 408 PRs')."""
    if not isinstance(raw, dict):
        return None

    # AI Spend / Issue, Headcount + AI Spend / Issue
    if "issues" in name.lower() and raw.get("spend_usd") is not None:
        spend = raw.get("spend_usd") or raw.get("ai_spend_usd", 0)
        issues = raw.get("issues_shipped") or raw.get("issues", 0)
        if issues:
            return f"${spend:,.0f} / {issues:,} issues"

    # Token Cost per Dev, Tokens per Dev
    if raw.get("headcount") and (raw.get("spend_usd") is not None or raw.get("tokens") is not None):
        hc = raw["headcount"]
        if raw.get("spend_usd") is not None:
            return f"${raw['spend_usd']:,.0f} / {hc} devs"
        if raw.get("tokens") is not None:
            tok = raw["tokens"]
            if tok >= 1_000_000:
                return f"{tok / 1_000_000:.1f}M tokens / {hc} devs"
            return f"{tok:,} tokens / {hc} devs"

    # AI Spend %
    if "ai_spend_usd" in raw and "engineering_spend_usd" in raw:
        return f"${raw['ai_spend_usd']:,.0f} / ${raw['engineering_spend_usd']:,.0f}"

    # Percentages with matched/total (AI-Assisted PRs, Weekly Active AI Users)
    if raw.get("matched_prs") is not None and raw.get("total_prs") is not None:
        return f"{raw['matched_prs']} / {raw['total_prs']} PRs"
    if raw.get("active_users") is not None and raw.get("headcount") is not None:
        return f"{raw['active_users']} / {raw['headcount']} devs"

    # AI Code Share
    if raw.get("ai_lines") is not None and raw.get("total_lines") is not None:
        ai = raw["ai_lines"]
        total = raw["total_lines"]
        if total >= 1000:
            return f"{ai:,} / {total:,} lines"

    # Defects per 100 Issues
    if raw.get("bugs_created") is not None and raw.get("issues_shipped") is not None:
        return f"{raw['bugs_created']} bugs / {raw['issues_shipped']} issues"

    # Growth Allocation
    if raw.get("planned_count") is not None and raw.get("total_count") is not None:
        return f"{raw['planned_count']} / {raw['total_count']} issues"

    # Generic numerator/denominator fallback
    if raw.get("numerator") is not None and raw.get("denominator") is not None:
        n, d = raw["numerator"], raw["denominator"]
        if d > 1:
            return f"{n:,.0f} / {d:,.0f}"

    return None


def scalar_from_parts(parts: MetricParts) -> float:
    """Comparable scalar: absolute when denom is 1 (or 0), else percentage (n/d)*100."""
    if parts.denominator == 0:
        return float(parts.numerator)
    if float(parts.denominator) == 1.0:
        return float(parts.numerator)
    return round((float(parts.numerator) / float(parts.denominator)) * 100.0, 1)


def is_off_target(
    value: float | None,
    *,
    target: float | None,
    direction: str | None,
    error: str | None = None,
) -> bool:
    """True when the row belongs in the off-target section."""
    if error or value is None:
        return True
    if target is None or direction is None:
        return False
    if direction == "lower":
        return value > target
    if direction == "higher":
        return value < target
    return False


def _metric_id_or_none(entry: dict[str, Any]) -> int | None:
    if not has_metric_id(entry):
        return None
    return int(entry["metric-id"])


def generate_digest_row(
    name: str,
    entry: dict[str, Any],
    *,
    registry: dict[str, Any],
    ctx: MetricUpsertContext,
) -> DigestRow:
    """Run one generator and classify against registry target/direction."""
    metric_id = _metric_id_or_none(entry)
    target = registry_metric_target(entry)
    description = registry_metric_description(entry)
    tags = tuple(registry_metric_tags(entry))
    try:
        unit = digest_display_unit(name, entry)
    except ValueError as e:
        return DigestRow(
            name=name,
            metric_id=metric_id,
            value=None,
            target=target,
            direction=None,
            off_target=True,
            error=str(e),
            description=description,
            tags=tags,
        )
    try:
        direction = registry_metric_direction(entry)
    except ValueError as e:
        return DigestRow(
            name=name,
            metric_id=metric_id,
            value=None,
            target=target,
            direction=None,
            off_target=True,
            error=str(e),
            description=description,
            tags=tags,
            unit=unit,
        )

    cfg_err = validate_metric_target_direction(entry)
    if cfg_err:
        return DigestRow(
            name=name,
            metric_id=metric_id,
            value=None,
            target=target,
            direction=direction,
            off_target=True,
            error=cfg_err,
            description=description,
            tags=tags,
            unit=unit,
        )

    gen_name = str(entry.get("metric-generator") or "").strip()
    raw: dict[str, Any] | None = None
    try:
        raw = invoke_metric_generator(gen_name, registry=registry, ctx=ctx)
        # Prefer explicit ``value`` when present so USD/issue (and similar) are not
        # misread as (numerator/denominator)*100 by :func:`scalar_from_parts`.
        if isinstance(raw, dict) and raw.get("value") is not None and not raw.get("error"):
            value = float(raw["value"])
        else:
            parts = parse_generator_parts(raw, metric_name=name, registry=registry)
            value = scalar_from_parts(parts)
        err = None
    except MetricUpsertError as e:
        value = None
        err = str(e)
    except Exception as e:  # noqa: BLE001 — surface integration failures in digest
        value = None
        err = f"{type(e).__name__}: {e}"

    context = _build_context_string(name, raw, unit) if isinstance(raw, dict) else None

    return DigestRow(
        name=name,
        metric_id=metric_id,
        value=value,
        target=target,
        direction=direction,
        off_target=is_off_target(value, target=target, direction=direction, error=err),
        error=err,
        description=description,
        tags=tags,
        unit=unit,
        context=context,
    )


def build_digest_rows(
    *,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
    tag: str | None = None,
) -> list[DigestRow]:
    """Generate a digest row for every registry metric that has a generator.

    When *tag* is set, only metrics carrying that registry tag are included
    (normalized via :func:`normalize_tag`, e.g. ``AKKR`` → ``akkr``).
    """
    reg = registry if registry is not None else load_metrics_registry()
    resolve_ctx = ctx or MetricUpsertContext(
        entry_date=date.today().isoformat(),
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
    tag_norm = normalize_tag(tag) if tag else ""
    rows: list[DigestRow] = []
    for name, entry in iter_metrics_with_generator(registry=reg):
        if tag_norm and not entry_has_tag(entry, tag_norm):
            continue
        rows.append(generate_digest_row(name, entry, registry=reg, ctx=resolve_ctx))
    return rows


def partition_digest_rows(rows: list[DigestRow]) -> tuple[list[DigestRow], list[DigestRow]]:
    """Split into (off_target alpha, rest alpha)."""
    off = sorted((r for r in rows if r.off_target), key=lambda r: r.name.casefold())
    rest = sorted((r for r in rows if not r.off_target), key=lambda r: r.name.casefold())
    return off, rest


@dataclass(frozen=True)
class DigestColumnWidths:
    name: int
    metric_id: int
    value: int
    target: int
    direction: int
    tags: int
    description: int

    @property
    def header(self) -> str:
        gap = " " * _DIGEST_COL_GAP
        return (
            f"{'NAME':<{self.name}}{gap}"
            f"{'ID':<{self.metric_id}}{gap}"
            f"{'VALUE':<{self.value}}{gap}"
            f"{'TARGET':<{self.target}}{gap}"
            f"{'DIR':<{self.direction}}{gap}"
            f"{'TAGS':<{self.tags}}{gap}"
            f"{'DESCRIPTION':<{self.description}}"
        )

    @property
    def rule(self) -> str:
        return "-" * len(self.header)


def column_widths_for_digest_rows(
    rows: list[DigestRow],
    *,
    min_total_width: int | None = None,  # noqa: ARG001 — kept for call-site compat; ignored
) -> DigestColumnWidths:
    """Compact natural column widths (no padding). Long VALUES wrap below the row."""
    name_w = len("NAME")
    id_w = len("ID")
    value_w = len("VALUE")
    target_w = len("TARGET")
    dir_w = len("DIR")
    tags_w = len("TAGS")
    desc_w = len("DESCRIPTION")
    for row in rows:
        name_w = max(name_w, len(row.name))
        id_w = max(id_w, len(row.id_display))
        target_w = max(target_w, len(row.target_display))
        dir_w = max(dir_w, len(row.direction or "—"))
        tags_w = max(tags_w, len(row.tags_display))
        if row.description:
            desc_w = max(
                desc_w,
                len(_truncate_digest_detail(row.description, max_len=_DIGEST_DESCRIPTION_MAX)),
            )
        vd = row.value_display
        if len(vd) <= _DIGEST_VALUE_INLINE_MAX:
            value_w = max(value_w, len(vd))
    value_w = min(max(value_w, len("VALUE")), _DIGEST_VALUE_INLINE_MAX)
    desc_w = min(desc_w, _DIGEST_DESCRIPTION_MAX)
    return DigestColumnWidths(
        name=name_w,
        metric_id=id_w,
        value=value_w,
        target=target_w,
        direction=dir_w,
        tags=tags_w,
        description=desc_w,
    )


def format_digest_lines(row: DigestRow, *, widths: DigestColumnWidths | None = None) -> list[str]:
    """Columnar primary row (description at end); errors follow on a detail line."""
    w = widths or column_widths_for_digest_rows([row])
    direction = row.direction or "—"
    gap = " " * _DIGEST_COL_GAP
    value = row.value_display
    overflow: str | None = None
    if len(value) > w.value:
        overflow = value
        value = ""
    desc = (
        _truncate_digest_detail(row.description, max_len=_DIGEST_DESCRIPTION_MAX)
        if row.description
        else "—"
    )
    primary = (
        f"{row.name:<{w.name}}{gap}"
        f"{row.id_display:<{w.metric_id}}{gap}"
        f"{value:<{w.value}}{gap}"
        f"{row.target_display:<{w.target}}{gap}"
        f"{direction:<{w.direction}}{gap}"
        f"{row.tags_display:<{w.tags}}{gap}"
        f"{desc:<{w.description}}"
    )
    lines = [primary]
    if row.error:
        lines.extend(_detail_lines(row.error))
    elif overflow:
        lines.extend(_detail_lines(overflow))
    return lines


def format_digest_line(row: DigestRow, *, widths: DigestColumnWidths | None = None) -> str:
    """Primary columnar line only (see :func:`format_digest_lines` for wrapped values)."""
    return format_digest_lines(row, widths=widths)[0]


def _format_section(
    title: str,
    section_rows: list[DigestRow],
    *,
    widths: DigestColumnWidths,
) -> list[str]:
    lines = [title, widths.header, widths.rule]
    if section_rows:
        for r in section_rows:
            lines.extend(format_digest_lines(r, widths=widths))
    else:
        lines.append("(none)")
    return lines


def format_digest_body(
    rows: list[DigestRow],
    *,
    as_of: str | None = None,
    overnight: list[OvernightJobOutcome] | None = None,
    tag: str | None = None,
) -> str:
    """Plain-text morning report: overnight jobs, then off-target KPIs, then the rest."""
    as_of_s = as_of or date.today().isoformat()
    off, rest = partition_digest_rows(rows)
    widths = column_widths_for_digest_rows(rows)
    tag_norm = normalize_tag(tag) if tag else ""
    title = f"Morning report — {as_of_s}"
    if tag_norm:
        title = f"{title}  (tag: {tag_norm})"
    lines: list[str] = [
        title,
        "",
    ]
    if not tag_norm:
        overnight_rows = overnight if overnight is not None else []
        lines.extend(format_overnight_jobs_section(overnight_rows))
        lines.append("")
    kpi_line = (
        f"KPIs — off target: {len(off)}  |  on target / other: {len(rest)}  |  total: {len(rows)}"
    )
    if tag_norm:
        kpi_line = f"{kpi_line}  |  filter: {tag_norm}"
    lines.append(kpi_line)
    lines.append("")
    lines.extend(_format_section("OFF TARGET", off, widths=widths))
    lines.append("")
    lines.extend(_format_section("ALL OTHER GENERATED METRICS", rest, widths=widths))
    lines.append("")
    return "\n".join(lines)


def format_digest_subject(
    rows: list[DigestRow],
    *,
    as_of: str | None = None,
    overnight: list[OvernightJobOutcome] | None = None,
    tag: str | None = None,
) -> str:
    as_of_s = as_of or date.today().isoformat()
    off_n = sum(1 for r in rows if r.off_target)
    job_fail_n = overnight_failure_count(overnight or [])
    tag_norm = normalize_tag(tag) if tag else ""
    tag_bit = f", tag {tag_norm}" if tag_norm else ""
    if job_fail_n:
        return f"Morning report {as_of_s} — {job_fail_n} job issue(s), {off_n} off target{tag_bit}"
    return f"Morning report {as_of_s} — {off_n} off target{tag_bit}"


@dataclass(frozen=True)
class DigestResult:
    rows: list[DigestRow]
    subject: str
    body: str
    sent: bool
    message_id: str | None = None
    error: str | None = None


def run_metrics_digest(
    *,
    dry_run: bool = False,
    days: int = 30,
    timeout_seconds: float = 120.0,
    as_of: str | None = None,
    registry: dict[str, Any] | None = None,
    send_fn: Any | None = None,
    skip_overnight: bool = False,
    overnight: list[OvernightJobOutcome] | None = None,
    tag: str | None = None,
) -> DigestResult:
    """Generate the morning report and optionally email via SES."""
    as_of_s = as_of or date.today().isoformat()
    as_of_date = date.fromisoformat(as_of_s)
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
    rows = build_digest_rows(registry=registry, ctx=ctx, tag=tag)
    tag_filter = bool(normalize_tag(tag)) if tag else False
    # Tag-filtered reports are KPI-only; omit overnight jobs section and CloudWatch fetch.
    if overnight is not None and not tag_filter:
        overnight_rows = overnight
    elif skip_overnight or tag_filter:
        overnight_rows = []
    else:
        overnight_rows = collect_overnight_job_outcomes(as_of=as_of_date)
    subject = format_digest_subject(rows, as_of=as_of_s, overnight=overnight_rows, tag=tag)
    body = format_digest_body(rows, as_of=as_of_s, overnight=overnight_rows, tag=tag)

    if dry_run:
        return DigestResult(rows=rows, subject=subject, body=body, sent=False)

    mailer = send_fn or send_email
    try:
        to = digest_email_recipients()
        from_addr = digest_email_from()
        result = mailer(to=to, subject=subject, body=body, from_addr=from_addr)
    except SesEmailError as e:
        return DigestResult(rows=rows, subject=subject, body=body, sent=False, error=str(e))
    except Exception as e:  # noqa: BLE001
        return DigestResult(
            rows=rows,
            subject=subject,
            body=body,
            sent=False,
            error=f"{type(e).__name__}: {e}",
        )

    message_id = None
    if isinstance(result, dict):
        message_id = result.get("message_id")
    return DigestResult(
        rows=rows,
        subject=subject,
        body=body,
        sent=True,
        message_id=str(message_id) if message_id else None,
    )


def generate_metrics_digest_deck(
    rows: list[DigestRow],
    *,
    tag: str | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Generate a Google Slides deck from digest rows.

    Creates/updates a persistent deck in Output (e.g., "AKKR Metrics") whose link
    never changes, and archives a dated copy to Historical Data/{YYYY-MM-DD}/.

    Returns dict with deck_id, deck_url, historical_url, and any error.
    """
    from .drive_config import (
        get_qbr_output_root_folder_id,
        _get_drive,
        list_files_by_name_in_folder,
        dedupe_duplicate_names_in_folder,
    )
    from .deck_presentation_api import create_presentation
    from .slide_requests import append_slide, append_text_box
    from .slide_primitives import background, rect
    from .slides_theme import (
        SLIDE_W, SLIDE_H, MARGIN, NAVY, WHITE, BLUE, LIGHT, FONT,
        BODY_Y, BODY_BOTTOM,
    )
    from .export_drive_layout import ensure_historical_day_folder, historical_day_folder_label

    as_of_s = as_of or date.today().isoformat()
    tag_label = tag.upper() if tag else "KPI"
    persistent_title = f"{tag_label} Metrics"  # stable name for Output
    historical_title = f"{tag_label} Metrics — {as_of_s}"  # dated name for Historical Data

    output_folder = get_qbr_output_root_folder_id()
    if not output_folder:
        return {"error": "No Output folder configured (set GOOGLE_QBR_GENERATOR_FOLDER_ID)"}

    try:
        drive_svc = _get_drive()

        # Look for existing persistent deck by name
        existing = list_files_by_name_in_folder(
            persistent_title,
            output_folder,
            mime_type="application/vnd.google-apps.presentation",
        )
        if existing:
            deck_id = str(existing[0]["id"])
            dedupe_duplicate_names_in_folder(output_folder, persistent_title)
            logger.info("Reusing persistent presentation %s: %s", deck_id, persistent_title)
        else:
            deck_id, err = create_presentation(drive_svc, persistent_title, output_folder_id=output_folder)
            if err or not deck_id:
                return {"error": f"Failed to create presentation: {err}"}
    except Exception as e:
        return {"error": f"Failed to create/find presentation: {e}"}

    # Get Slides service and clear existing slides if reusing
    from .slides_api import _get_service

    slides_svc, _, _ = _get_service()
    try:
        pres = slides_svc.presentations().get(presentationId=deck_id).execute()
        existing_slides = pres.get("slides") or []
        if len(existing_slides) > 1 or (existing_slides and existing_slides[0].get("objectId") != "p"):
            delete_reqs = [{"deleteObject": {"objectId": s["objectId"]}} for s in existing_slides]
            slides_svc.presentations().batchUpdate(
                presentationId=deck_id, body={"requests": delete_reqs}
            ).execute()
            logger.info("Cleared %d existing slides from %s", len(existing_slides), persistent_title)
    except Exception as e:
        logger.warning("Could not clear existing slides: %s", e)

    reqs: list[dict[str, Any]] = []

    # Title slide — shows dated title as content (index 0 since we cleared all slides)
    title_sid = "metrics_title"
    append_slide(reqs, title_sid, 0)
    background(reqs, title_sid, NAVY)
    append_text_box(reqs, f"{title_sid}_h", title_sid, MARGIN, 140, SLIDE_W - 2 * MARGIN, 60, historical_title)
    reqs.append({
        "updateTextStyle": {
            "objectId": f"{title_sid}_h",
            "style": {
                "fontFamily": FONT,
                "fontSize": {"magnitude": 32, "unit": "PT"},
                "bold": True,
                "foregroundColor": {"opaqueColor": {"rgbColor": WHITE}},
            },
            "textRange": {"type": "ALL"},
            "fields": "fontFamily,fontSize,bold,foregroundColor",
        }
    })
    subtitle = f"{len(rows)} metrics • as of {as_of_s}"
    append_text_box(reqs, f"{title_sid}_sub", title_sid, MARGIN, 210, SLIDE_W - 2 * MARGIN, 30, subtitle)
    reqs.append({
        "updateTextStyle": {
            "objectId": f"{title_sid}_sub",
            "style": {
                "fontFamily": FONT,
                "fontSize": {"magnitude": 16, "unit": "PT"},
                "foregroundColor": {"opaqueColor": {"rgbColor": {"red": 0.7, "green": 0.8, "blue": 0.9}}},
            },
            "textRange": {"type": "ALL"},
            "fields": "fontFamily,fontSize,foregroundColor",
        }
    })

    # Split rows: off-target first, then on-target
    off_target = [r for r in rows if r.off_target]
    on_target = [r for r in rows if not r.off_target]

    # KPI slides — 6 metrics per slide
    all_rows = off_target + on_target
    metrics_per_slide = 6
    slide_idx = 1  # starts at 1 since title slide is at index 0

    for chunk_start in range(0, len(all_rows), metrics_per_slide):
        chunk = all_rows[chunk_start : chunk_start + metrics_per_slide]
        sid = f"metrics_s{slide_idx}"
        append_slide(reqs, sid, slide_idx)
        background(reqs, sid, WHITE)

        # Header bar
        rect(reqs, f"{sid}_hdr", sid, 0, 0, SLIDE_W, 48, NAVY)
        section = "Off Target" if chunk_start < len(off_target) else "On Target"
        page_label = f"{tag_label} Metrics — {section}"
        append_text_box(reqs, f"{sid}_htxt", sid, MARGIN, 12, SLIDE_W - 2 * MARGIN, 28, page_label)
        reqs.append({
            "updateTextStyle": {
                "objectId": f"{sid}_htxt",
                "style": {
                    "fontFamily": FONT,
                    "fontSize": {"magnitude": 20, "unit": "PT"},
                    "bold": True,
                    "foregroundColor": {"opaqueColor": {"rgbColor": WHITE}},
                },
                "textRange": {"type": "ALL"},
                "fields": "fontFamily,fontSize,bold,foregroundColor",
            }
        })

        # KPI cards — 2 columns x 3 rows
        card_w = (SLIDE_W - 3 * MARGIN) / 2
        card_h = 105  # taller to fit context line
        y_start = 56
        gap = 6

        for i, row in enumerate(chunk):
            col = i % 2
            row_idx = i // 2
            cx = MARGIN + col * (card_w + MARGIN)
            cy = y_start + row_idx * (card_h + gap)

            # Card background
            card_fill = {"red": 1.0, "green": 0.95, "blue": 0.95} if row.off_target else LIGHT
            rect(reqs, f"{sid}_c{i}", sid, cx, cy, card_w, card_h, card_fill)

            # Metric name
            append_text_box(reqs, f"{sid}_n{i}", sid, cx + 8, cy + 5, card_w - 16, 18, row.name)
            reqs.append({
                "updateTextStyle": {
                    "objectId": f"{sid}_n{i}",
                    "style": {
                        "fontFamily": FONT,
                        "fontSize": {"magnitude": 11, "unit": "PT"},
                        "bold": True,
                        "foregroundColor": {"opaqueColor": {"rgbColor": NAVY}},
                    },
                    "textRange": {"type": "ALL"},
                    "fields": "fontFamily,fontSize,bold,foregroundColor",
                }
            })

            # Value
            value_color = {"red": 0.8, "green": 0.2, "blue": 0.2} if row.off_target else BLUE
            append_text_box(reqs, f"{sid}_v{i}", sid, cx + 8, cy + 26, card_w - 16, 32, row.value_display)
            reqs.append({
                "updateTextStyle": {
                    "objectId": f"{sid}_v{i}",
                    "style": {
                        "fontFamily": FONT,
                        "fontSize": {"magnitude": 26, "unit": "PT"},
                        "bold": True,
                        "foregroundColor": {"opaqueColor": {"rgbColor": value_color}},
                    },
                    "textRange": {"type": "ALL"},
                    "fields": "fontFamily,fontSize,bold,foregroundColor",
                }
            })

            # Context line (e.g. "$9,950 / 288 issues")
            context_text = row.context or ""
            if context_text:
                append_text_box(reqs, f"{sid}_x{i}", sid, cx + 8, cy + 58, card_w - 16, 16, context_text)
                reqs.append({
                    "updateTextStyle": {
                        "objectId": f"{sid}_x{i}",
                        "style": {
                            "fontFamily": FONT,
                            "fontSize": {"magnitude": 9, "unit": "PT"},
                            "foregroundColor": {"opaqueColor": {"rgbColor": {"red": 0.35, "green": 0.35, "blue": 0.4}}},
                        },
                        "textRange": {"type": "ALL"},
                        "fields": "fontFamily,fontSize,foregroundColor",
                    }
                })

            # Target line
            dir_arrow = "↑" if row.direction == "higher" else "↓" if row.direction == "lower" else ""
            target_text = f"Target: {row.target_display} {dir_arrow}".strip()
            target_y = cy + 76 if context_text else cy + 70
            append_text_box(reqs, f"{sid}_t{i}", sid, cx + 8, cy + 86, card_w - 16, 16, target_text)
            reqs.append({
                "updateTextStyle": {
                    "objectId": f"{sid}_t{i}",
                    "style": {
                        "fontFamily": FONT,
                        "fontSize": {"magnitude": 9, "unit": "PT"},
                        "foregroundColor": {"opaqueColor": {"rgbColor": {"red": 0.4, "green": 0.4, "blue": 0.45}}},
                    },
                    "textRange": {"type": "ALL"},
                    "fields": "fontFamily,fontSize,foregroundColor",
                }
            })

        slide_idx += 1

    # Execute batch update
    if reqs:
        slides_svc.presentations().batchUpdate(
            presentationId=deck_id, body={"requests": reqs}
        ).execute()

    # Delete default blank slide(s) that Google auto-creates
    try:
        pres = slides_svc.presentations().get(presentationId=deck_id).execute()
        slides_in_pres = pres.get("slides") or []
        our_slide_ids = {"metrics_title"} | {f"metrics_s{i}" for i in range(1, slide_idx)}
        delete_reqs = []
        for s in slides_in_pres:
            sid = s.get("objectId", "")
            if sid and sid not in our_slide_ids:
                delete_reqs.append({"deleteObject": {"objectId": sid}})
        if delete_reqs:
            slides_svc.presentations().batchUpdate(
                presentationId=deck_id, body={"requests": delete_reqs}
            ).execute()
            logger.info("Deleted %d default/stale slide(s)", len(delete_reqs))
    except Exception as e:
        logger.warning("Could not clean up default slides: %s", e)

    deck_url = f"https://docs.google.com/presentation/d/{deck_id}/edit"

    # Copy to Historical Data/{YYYY-MM-DD}/ with dated title
    historical_url: str | None = None
    try:
        from .export_drive_layout import ensure_portfolio_output_folders

        folders = ensure_portfolio_output_folders()
        historical_folder_id = folders.get("historical_folder_id")
        if historical_folder_id:
            day = date.fromisoformat(as_of_s) if as_of_s else date.today()
            day_folder_id = ensure_historical_day_folder(historical_folder_id, day)
            day_label = historical_day_folder_label(day)

            # Remove any existing copy with same dated name
            for old in list_files_by_name_in_folder(
                historical_title,
                day_folder_id,
                mime_type="application/vnd.google-apps.presentation",
            ):
                old_id = str(old.get("id") or "")
                if old_id:
                    drive_svc.files().update(fileId=old_id, body={"trashed": True}).execute()

            # Copy persistent deck to Historical Data
            copied = drive_svc.files().copy(
                fileId=deck_id,
                body={"name": historical_title, "parents": [day_folder_id]},
                fields="id",
            ).execute()
            historical_id = str(copied["id"])
            historical_url = f"https://docs.google.com/presentation/d/{historical_id}/edit"
            logger.info(
                "Copied metrics deck → Historical Data/%s/%s",
                day_label,
                historical_title,
            )
    except Exception as e:
        logger.warning("Could not copy to Historical Data: %s", e)

    result: dict[str, Any] = {"deck_id": deck_id, "deck_url": deck_url}
    if historical_url:
        result["historical_url"] = historical_url
    return result


def add_metrics_digest_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--dry-run",
        "--no-send",
        action="store_true",
        dest="dry_run",
        help="Print morning report only; do not send email",
    )
    ap.add_argument("--days", type=int, default=30, help="Trailing window for generators (default: 30)")
    ap.add_argument("--timeout", type=float, default=120.0, metavar="SEC")
    ap.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Report as-of date (default: today)",
    )
    ap.add_argument(
        "--tag",
        default=None,
        metavar="TAG",
        help=(
            "Only include KPIs with this registry tag (e.g. akkr, mfr, engineering); "
            "omits the overnight jobs section"
        ),
    )
    ap.add_argument(
        "--skip-overnight",
        action="store_true",
        help="Omit last-night's jobs section (skip CloudWatch)",
    )
    ap.add_argument("-v", "--verbose", action="store_true")


def run_metrics_digest_cli(argv: Sequence[str] | None = None, *, prog: str = "metrics-digest") -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Morning report: last night's ECS jobs, then live my-metrics.yaml KPIs "
            "(emailed via SES unless --dry-run)."
        ),
    )
    add_metrics_digest_arguments(ap)
    ns = ap.parse_args(list(argv) if argv is not None else None)
    if ns.verbose:
        logging.getLogger("cortex").setLevel(logging.INFO)

    result = run_metrics_digest(
        dry_run=bool(ns.dry_run),
        days=int(ns.days),
        timeout_seconds=float(ns.timeout),
        as_of=str(ns.date),
        skip_overnight=bool(ns.skip_overnight),
        tag=(str(ns.tag).strip() or None) if ns.tag else None,
    )
    print(result.subject)
    print()
    print(result.body)
    if result.error:
        print(f"Error: {result.error}", file=sys.stderr)
        return 1
    if result.sent:
        mid = result.message_id or "(no MessageId)"
        print(f"Sent via SES message_id={mid}", file=sys.stderr)
    elif ns.dry_run:
        print("(dry-run — email not sent)", file=sys.stderr)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run_metrics_digest_cli(argv, prog="metrics-digest")


if __name__ == "__main__":
    raise SystemExit(main())
