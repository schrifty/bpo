#!/usr/bin/env python3
"""Full CS Report week dump per CSR customer (Sheet + short markdown).

Usage:
  cortex --export-csr [--customer NAME] [--slot 0600] [--no-drive] [--out-dir DIR]
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from .config import logger
from .config_paths import CS_REPORT_CUSTOMER_ALIASES_FILE
from .cs_report_client import (
    _load_cohort_customer_alias_map,
    csr_latest_report_meta,
    csr_site_entries_for_exact_week_customer,
    csr_sites_and_columns_for_export,
    distinct_csr_week_customers,
    load_latest_csr_week_rows,
)
from .export_drive_layout import (
    CSR_DUMP_RUN_SLOTS,
    CUSTOMER_EXPORTS_FOLDER,
    historical_run_slot_label,
    upload_csr_markdown_and_spreadsheet,
)
from .export_pendo_spreadsheet import _cell_value
from .export_run_diagnostics import export_diagnostics_scope, export_phase

_CHICAGO = ZoneInfo("America/Chicago")
# EventBridge UTC hours locked to current CDT local times (midnight / 6am / noon / 6pm).
CSR_DUMP_UTC_HOUR_TO_SLOT: dict[int, str] = {5: "0000", 11: "0600", 17: "1200", 23: "1800"}
_CSR_DUMP_STEM_SUFFIX = " CSR Dump"


def csr_dump_stem(csr_customer: str) -> str:
    name = (csr_customer or "").strip() or "customer"
    return f"{name}{_CSR_DUMP_STEM_SUFFIX}"


def chicago_export_date(now: dt.datetime | None = None) -> dt.date:
    stamp = now or dt.datetime.now(dt.timezone.utc)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    return stamp.astimezone(_CHICAGO).date()


def infer_csr_dump_slot(now: dt.datetime | None = None) -> str:
    stamp = now or dt.datetime.now(dt.timezone.utc)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    hour = stamp.astimezone(dt.timezone.utc).hour
    if hour in CSR_DUMP_UTC_HOUR_TO_SLOT:
        return CSR_DUMP_UTC_HOUR_TO_SLOT[hour]
    hours = sorted(CSR_DUMP_UTC_HOUR_TO_SLOT)
    nearest = min(hours, key=lambda h: min(abs(h - hour), 24 - abs(h - hour)))
    return CSR_DUMP_UTC_HOUR_TO_SLOT[nearest]


def customer_exports_folder_for_csr_name(csr_customer: str) -> str:
    """Map a CSR workbook customer string onto ``Customer Exports/{folder}``.

    Prefers Pendo/cohort prefixes, then CS Report YAML keys (original casing).
    Unmapped names keep the workbook string.
    """
    needle = (csr_customer or "").strip()
    if not needle:
        return needle
    lower = needle.lower()
    cohort = _load_cohort_customer_alias_map()
    terms = cohort.get(lower)
    if terms:
        return str(terms[0]).strip() or needle
    if CS_REPORT_CUSTOMER_ALIASES_FILE.is_file():
        raw = yaml.safe_load(CS_REPORT_CUSTOMER_ALIASES_FILE.read_text(encoding="utf-8")) or {}
        if isinstance(raw, dict):
            for key, vals in raw.items():
                orig = str(key).strip()
                if not orig or orig.startswith("#"):
                    continue
                names = [orig]
                if isinstance(vals, str):
                    names.append(vals)
                elif isinstance(vals, list):
                    names.extend(str(v).strip() for v in vals if str(v).strip())
                if lower in {n.lower() for n in names if n}:
                    return orig
    return needle


def csr_dump_folder_was_mapped(csr_customer: str, folder: str) -> bool:
    return (folder or "").strip().lower() != (csr_customer or "").strip().lower()


def build_csr_dump_tables(
    *,
    csr_customer: str,
    folder: str,
    slot: str,
    export_date: dt.date,
    sites: list[dict[str, Any]],
    presented: list[dict[str, Any]],
    columns: list[str],
    source_meta: dict[str, str],
) -> dict[str, list[list[Any]]]:
    exported = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta_rows: list[list[Any]] = [
        ["field", "value"],
        ["csr_customer", csr_customer],
        ["customer_exports_folder", folder],
        ["delta", "week"],
        ["run_slot", slot],
        ["export_date_chicago", export_date.isoformat()],
        ["exported_utc", exported],
        ["factory_count", len(sites)],
        ["source_file", source_meta.get("file") or ""],
        ["source_modified", source_meta.get("modified") or ""],
    ]
    factory_grid: list[list[Any]] = [list(columns)]
    for row in presented:
        factory_grid.append([_cell_value(row.get(col)) for col in columns])
    if len(factory_grid) == 1:
        factory_grid.append([""] * max(len(columns), 1) if columns else [""])
    return {"meta": meta_rows, "factories": factory_grid}


def render_csr_dump_markdown(
    *,
    csr_customer: str,
    folder: str,
    slot: str,
    export_date: dt.date,
    factory_count: int,
    source_meta: dict[str, str],
    spreadsheet_url: str = "",
    mapped: bool = True,
) -> str:
    lines = [
        f"# {csr_customer} CSR Dump",
        "",
        f"- **CSR customer:** {csr_customer}",
        f"- **Customer Exports folder:** `{CUSTOMER_EXPORTS_FOLDER}/{folder}/`",
        f"- **Delta:** week",
        f"- **Run slot:** `{slot}` (America/Chicago wall clock; not in the filename)",
        f"- **Chicago date:** {export_date.isoformat()}",
        f"- **Exported (UTC):** {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"- **Factories:** {factory_count}",
        f"- **Source workbook:** {source_meta.get('file') or '(unknown)'}",
        f"- **Source modified:** {source_meta.get('modified') or '(unknown)'}",
    ]
    if spreadsheet_url:
        lines.append(f"- **Spreadsheet:** {spreadsheet_url}")
    lines.append(
        f"- **Historical snapshot:** `Historical Data/{export_date.isoformat()}/{slot}/`"
    )
    if not mapped:
        lines.append(
            "- **Join note:** no Pendo/cohort/CS Report alias matched this CSR name; "
            "folder uses the workbook customer string. Salesforce remains system of record "
            "for commercial status — this dump is CS Report inventory only."
        )
    lines.extend(
        [
            "",
            "This index is a pointer to the Google Sheet. Factory metrics live on the "
            "**factories** tab (CSR UI column labels, `delta=week` only).",
            "",
        ]
    )
    return "\n".join(lines)


def _customers_for_run(
    all_customers: list[str],
    *,
    customer_filter: str | None,
) -> list[str]:
    if not customer_filter:
        return list(all_customers)
    needle = customer_filter.strip()
    if not needle:
        return list(all_customers)
    lower = needle.lower()
    matched = [
        name
        for name in all_customers
        if name.lower() == lower or customer_exports_folder_for_csr_name(name).lower() == lower
    ]
    if not matched:
        raise RuntimeError(
            f"No CS Report week customer matched --customer {customer_filter!r} "
            f"(tried exact CSR name and Customer Exports folder mapping)"
        )
    return matched


def export_csr_dumps(
    *,
    slot: str,
    customer: str | None = None,
    no_drive: bool = False,
    out_dir: Path | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    slot_label = historical_run_slot_label(slot)
    export_date = chicago_export_date(now)
    with export_phase("csr-dump-load"):
        rows = load_latest_csr_week_rows()
    if not rows:
        raise RuntimeError(
            "CS Report workbook missing or empty — cannot build CSR dumps (fail loud)"
        )
    all_customers = distinct_csr_week_customers(rows)
    if not all_customers:
        raise RuntimeError("CS Report has no delta=week rows — cannot build CSR dumps")
    targets = _customers_for_run(all_customers, customer_filter=customer)
    source_meta = csr_latest_report_meta()
    failures: list[str] = []
    uploaded: list[dict[str, Any]] = []
    local_dir = out_dir or Path("output") / "csr-dump"
    migrated = False

    for csr_name in targets:
        folder = customer_exports_folder_for_csr_name(csr_name)
        mapped = csr_dump_folder_was_mapped(csr_name, folder)
        if not mapped:
            logger.warning(
                "CSR dump: no alias match for workbook customer %r; using folder %r "
                "(Salesforce remains SoR for commercial status)",
                csr_name,
                folder,
            )
        try:
            sites = csr_site_entries_for_exact_week_customer(csr_name, rows)
            presented, columns = csr_sites_and_columns_for_export(sites)
            stem = csr_dump_stem(csr_name)
            tables = build_csr_dump_tables(
                csr_customer=csr_name,
                folder=folder,
                slot=slot_label,
                export_date=export_date,
                sites=sites,
                presented=presented,
                columns=columns,
                source_meta=source_meta,
            )
            if no_drive:
                from .export_customer_pendo_snapshot import _write_local

                md = render_csr_dump_markdown(
                    csr_customer=csr_name,
                    folder=folder,
                    slot=slot_label,
                    export_date=export_date,
                    factory_count=len(sites),
                    source_meta=source_meta,
                    mapped=mapped,
                )
                dest = local_dir / folder / slot_label / f"{stem}.md"
                dest.parent.mkdir(parents=True, exist_ok=True)
                _write_local(dest, md)
                uploaded.append({"customer": csr_name, "folder": folder, "local_md": str(dest)})
                continue

            from .drive_config import upload_text_file_to_drive_folder
            from .export_drive_layout import ensure_customer_export_folders, persistent_filename
            from .export_output_archive import maybe_migrate_export_layout_on_startup

            if not migrated:
                maybe_migrate_export_layout_on_startup()
                migrated = True
            folders = ensure_customer_export_folders(folder)
            md = render_csr_dump_markdown(
                csr_customer=csr_name,
                folder=folder,
                slot=slot_label,
                export_date=export_date,
                factory_count=len(sites),
                source_meta=source_meta,
                mapped=mapped,
            )
            urls = upload_csr_markdown_and_spreadsheet(
                stem=stem,
                md=md,
                tables=tables,
                persistent_folder_id=folders["persistent_folder_id"],
                historical_folder_id=folders["historical_folder_id"],
                base_label=folders["base_label"],
                slot=slot_label,
                export_date=export_date,
            )
            md = render_csr_dump_markdown(
                csr_customer=csr_name,
                folder=folder,
                slot=slot_label,
                export_date=export_date,
                factory_count=len(sites),
                source_meta=source_meta,
                spreadsheet_url=urls.get("persistent_spreadsheet_url") or "",
                mapped=mapped,
            )
            upload_text_file_to_drive_folder(
                persistent_filename(stem, ext=".md"),
                md,
                folders["persistent_folder_id"],
                mime_type="text/markdown",
            )
            slot_folder_id = urls.get("historical_slot_folder_id")
            if slot_folder_id:
                from .export_drive_layout import historical_snapshot_filename

                upload_text_file_to_drive_folder(
                    historical_snapshot_filename(stem, ext=".md"),
                    md,
                    slot_folder_id,
                    mime_type="text/markdown",
                )
            uploaded.append({"customer": csr_name, "folder": folder, **urls})
        except Exception as exc:
            logger.exception("CSR dump failed for %s", csr_name)
            failures.append(f"{csr_name}: {exc}")

    result = {
        "slot": slot_label,
        "export_date": export_date.isoformat(),
        "customers": len(targets),
        "uploaded": len(uploaded),
        "failures": failures,
    }
    if failures:
        raise RuntimeError(
            f"CSR dump incomplete: {len(failures)}/{len(targets)} customer(s) failed: "
            + "; ".join(failures[:8])
        )
    logger.info(
        "CSR dump complete: %d customer(s) slot=%s chicago_date=%s",
        len(uploaded),
        slot_label,
        export_date.isoformat(),
    )
    return result


def export_csr_main(argv: list[str] | None = None, *, prog: str = "cortex --export-csr") -> None:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("--customer", help="CSR workbook name or Customer Exports folder")
    parser.add_argument(
        "--slot",
        choices=list(CSR_DUMP_RUN_SLOTS),
        default=None,
        help="Scheduled run slot (HHmm). Default: infer from current UTC hour.",
    )
    parser.add_argument("--no-drive", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    slot = args.slot or infer_csr_dump_slot()
    with export_diagnostics_scope("export-csr"):
        export_csr_dumps(
            slot=slot,
            customer=args.customer,
            no_drive=args.no_drive,
            out_dir=args.out_dir,
        )


if __name__ == "__main__":
    export_csr_main(sys.argv[1:])
