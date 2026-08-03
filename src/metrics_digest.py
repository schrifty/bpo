"""Daily KPI digest: live-generate registry metrics with generators and email results.

Off-target metrics (vs ``target`` / ``direction`` in ``config/my-metrics.yaml``) are
listed first; remaining metrics follow alphabetically after a separator.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
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
from src.ses_email import SesEmailError, digest_email_from, digest_email_recipients, send_email

logger = logging.getLogger("cortex")

SEPARATOR = "-" * 72


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


def format_digest_line(row: DigestRow) -> str:
    direction = row.direction or "—"
    return (
        f"{row.name}  id={row.id_display}  value={row.value_display}  "
        f"target={row.target_display}  direction={direction}"
    )


def format_digest_body(
    rows: list[DigestRow],
    *,
    as_of: str | None = None,
) -> str:
    """Plain-text email body with off-target section then separator then the rest."""
    as_of_s = as_of or date.today().isoformat()
    off, rest = partition_digest_rows(rows)
    lines: list[str] = [
        f"KPI digest — {as_of_s}",
        f"Off target: {len(off)}  |  On target / other: {len(rest)}  |  Total: {len(rows)}",
        "",
        "OFF TARGET",
        SEPARATOR,
    ]
    if off:
        lines.extend(format_digest_line(r) for r in off)
    else:
        lines.append("(none)")
    lines.extend(["", "ALL OTHER GENERATED METRICS", SEPARATOR])
    if rest:
        lines.extend(format_digest_line(r) for r in rest)
    else:
        lines.append("(none)")
    lines.append("")
    return "\n".join(lines)


def format_digest_subject(rows: list[DigestRow], *, as_of: str | None = None) -> str:
    as_of_s = as_of or date.today().isoformat()
    off_n = sum(1 for r in rows if r.off_target)
    return f"KPI digest {as_of_s} — {off_n} off target"


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
) -> DigestResult:
    """Generate digest rows and optionally email via SES."""
    as_of_s = as_of or date.today().isoformat()
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
    subject = format_digest_subject(rows, as_of=as_of_s)
    body = format_digest_body(rows, as_of=as_of_s)

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
        help="Print digest only; do not send email",
    )
    ap.add_argument("--days", type=int, default=30, help="Trailing window for generators (default: 30)")
    ap.add_argument("--timeout", type=float, default=120.0, metavar="SEC")
    ap.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Digest as-of date in subject/body (default: today)",
    )
    ap.add_argument("-v", "--verbose", action="store_true")


def run_metrics_digest_cli(argv: Sequence[str] | None = None, *, prog: str = "metrics-digest") -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Live-generate my-metrics.yaml KPIs with generators and email a morning digest.",
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
