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
    has_metric_id,
    iter_metrics_with_generator,
    load_metrics_registry,
    registry_metric_direction,
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

# Soft wrap width for long VALUE continuation lines. Primary table columns use
# compact natural widths only — never pad to fill a target width.
DIGEST_LINE_WIDTH = 128
_DIGEST_COL_GAP = 2
_DIGEST_COL_COUNT = 5
# Inline VALUE column cap — longer values print in full on following lines.
_DIGEST_VALUE_INLINE_MAX = 24


def _wrap_digest_text(text: str, *, width: int, prefix: str = "  ") -> list[str]:
    """Hard-wrap *text* to *width* without dropping characters."""
    avail = max(1, width - len(prefix))
    if len(text) <= avail:
        return [f"{prefix}{text}"]
    lines: list[str] = []
    rest = text
    while rest:
        lines.append(f"{prefix}{rest[:avail]}")
        rest = rest[avail:]
    return lines



@dataclass(frozen=True)
class DigestRow:
    name: str
    metric_id: int | None
    value: float | None
    target: float | None
    direction: str | None
    off_target: bool
    error: str | None = None

    @property
    def id_display(self) -> str:
        return str(self.metric_id) if self.metric_id is not None else "—"

    @property
    def value_display(self) -> str:
        if self.error:
            return f"error: {self.error}"
        if self.value is None:
            return "—"
        if float(self.value).is_integer():
            return str(int(self.value))
        return f"{self.value:.2f}".rstrip("0").rstrip(".")

    @property
    def target_display(self) -> str:
        if self.target is None:
            return "—"
        if float(self.target).is_integer():
            return str(int(self.target))
        return f"{self.target:.2f}".rstrip("0").rstrip(".")


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
        )

    gen_name = str(entry.get("metric-generator") or "").strip()
    try:
        raw = invoke_metric_generator(gen_name, registry=registry, ctx=ctx)
        parts = parse_generator_parts(raw, metric_name=name, registry=registry)
        value = scalar_from_parts(parts)
        err = None
    except MetricUpsertError as e:
        value = None
        err = str(e)
    except Exception as e:  # noqa: BLE001 — surface integration failures in digest
        value = None
        err = f"{type(e).__name__}: {e}"

    return DigestRow(
        name=name,
        metric_id=metric_id,
        value=value,
        target=target,
        direction=direction,
        off_target=is_off_target(value, target=target, direction=direction, error=err),
        error=err,
    )


def build_digest_rows(
    *,
    registry: dict[str, Any] | None = None,
    ctx: MetricUpsertContext | None = None,
) -> list[DigestRow]:
    """Generate a digest row for every registry metric that has a generator."""
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
    rows: list[DigestRow] = []
    for name, entry in iter_metrics_with_generator(registry=reg):
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

    @property
    def header(self) -> str:
        gap = " " * _DIGEST_COL_GAP
        return (
            f"{'NAME':<{self.name}}{gap}"
            f"{'ID':<{self.metric_id}}{gap}"
            f"{'VALUE':<{self.value}}{gap}"
            f"{'TARGET':<{self.target}}{gap}"
            f"{'DIR':<{self.direction}}"
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
    for row in rows:
        name_w = max(name_w, len(row.name))
        id_w = max(id_w, len(row.id_display))
        target_w = max(target_w, len(row.target_display))
        dir_w = max(dir_w, len(row.direction or "—"))
        vd = row.value_display
        if len(vd) <= _DIGEST_VALUE_INLINE_MAX:
            value_w = max(value_w, len(vd))
    value_w = min(max(value_w, len("VALUE")), _DIGEST_VALUE_INLINE_MAX)
    return DigestColumnWidths(
        name=name_w,
        metric_id=id_w,
        value=value_w,
        target=target_w,
        direction=dir_w,
    )


def format_digest_lines(row: DigestRow, *, widths: DigestColumnWidths | None = None) -> list[str]:
    """Columnar row lines: primary table row, plus wrapped full VALUE when it does not fit inline."""
    w = widths or column_widths_for_digest_rows([row])
    direction = row.direction or "—"
    gap = " " * _DIGEST_COL_GAP
    value = row.value_display
    if len(value) <= w.value:
        primary = (
            f"{row.name:<{w.name}}{gap}"
            f"{row.id_display:<{w.metric_id}}{gap}"
            f"{value:<{w.value}}{gap}"
            f"{row.target_display:<{w.target}}{gap}"
            f"{direction:<{w.direction}}"
        )
        return [primary]
    primary = (
        f"{row.name:<{w.name}}{gap}"
        f"{row.id_display:<{w.metric_id}}{gap}"
        f"{'':<{w.value}}{gap}"
        f"{row.target_display:<{w.target}}{gap}"
        f"{direction:<{w.direction}}"
    )
    wrap_width = max(DIGEST_LINE_WIDTH, len(primary))
    return [primary, *_wrap_digest_text(value, width=wrap_width, prefix="  ")]


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
) -> str:
    """Plain-text morning report: overnight jobs, then off-target KPIs, then the rest."""
    as_of_s = as_of or date.today().isoformat()
    off, rest = partition_digest_rows(rows)
    widths = column_widths_for_digest_rows(rows)
    lines: list[str] = [
        f"Morning report — {as_of_s}",
        "",
    ]
    overnight_rows = overnight if overnight is not None else []
    lines.extend(format_overnight_jobs_section(overnight_rows))
    lines.append("")
    lines.append(
        f"KPIs — off target: {len(off)}  |  on target / other: {len(rest)}  |  total: {len(rows)}"
    )
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
) -> str:
    as_of_s = as_of or date.today().isoformat()
    off_n = sum(1 for r in rows if r.off_target)
    job_fail_n = overnight_failure_count(overnight or [])
    if job_fail_n:
        return f"Morning report {as_of_s} — {job_fail_n} job issue(s), {off_n} off target"
    return f"Morning report {as_of_s} — {off_n} off target"


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
    rows = build_digest_rows(registry=registry, ctx=ctx)
    if overnight is not None:
        overnight_rows = overnight
    elif skip_overnight:
        overnight_rows = []
    else:
        overnight_rows = collect_overnight_job_outcomes(as_of=as_of_date)
    subject = format_digest_subject(rows, as_of=as_of_s, overnight=overnight_rows)
    body = format_digest_body(rows, as_of=as_of_s, overnight=overnight_rows)

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
