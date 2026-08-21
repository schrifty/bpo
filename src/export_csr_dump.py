#!/usr/bin/env python3
"""Full CS Report week dump per CSR customer at site, BU, and entity grain.

Each grain gets a dated Google Sheet plus a markdown twin carrying the same rows.

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
    _CSR_KPI_DEC1_FIELDS,
    _CSR_KPI_DEC2_FIELDS,
    _CSR_KPI_INT_FIELDS,
    _CSR_KPI_ROUND_FIELDS,
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
    csr_dump_report_title,
    csr_report_date_label,
    historical_run_slot_label,
    upload_csr_markdown_and_spreadsheet,
)
from .export_pendo_spreadsheet import _cell_value
from .export_run_diagnostics import ExportRunDiagnostics, export_diagnostics_scope, export_phase

_CHICAGO = ZoneInfo("America/Chicago")
# EventBridge UTC hours locked to current CDT local times (midnight / 6am / noon / 6pm).
CSR_DUMP_UTC_HOUR_TO_SLOT: dict[int, str] = {5: "0000", 11: "0600", 17: "1200", 23: "1800"}
_CSR_DUMP_STEM_SUFFIX = " CSR Dump"
_CSR_SUM_KEYS = frozenset(k for _, k in (_CSR_KPI_INT_FIELDS + _CSR_KPI_ROUND_FIELDS))
_CSR_MEAN_KEYS = frozenset(
    k for _, k in (_CSR_KPI_DEC1_FIELDS + _CSR_KPI_DEC2_FIELDS)
) | {"automated_health_composite"}
_CSR_HEALTH_RANK = {"RED": 3, "YELLOW": 2, "GREEN": 1, "NONE": 0}
_CSR_ROLLUP_DROP = {
    "bu": frozenset({"factory", "site", "entity", "factory_ndx"}),
    "entity": frozenset({"factory", "site", "factory_ndx"}),
}
_CSR_ROLLUP_GROUP_KEY = {"bu": "business_unit", "entity": "entity"}
_CSR_ROLLUP_TAB = {"site": "factories", "bu": "business_units", "entity": "entities"}
_CSR_LEVEL_LABEL = {"site": "site", "bu": "business unit", "entity": "entity"}
_CSR_ROLLUP_NOTE = (
    "BU and Entity sheets sum counts and dollar KPIs from site rows; percents, DOI, "
    "and similar rates are unweighted means of sites that have a value. These are not "
    "native LeanDNA CSR rollups."
)


def csr_dump_stem(csr_customer: str) -> str:
    """Legacy stem kept for leftover ``{Customer} CSR Dump`` files in Drive."""
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


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _consensus(values: list[Any]) -> Any:
    nonempty = [v for v in values if v not in (None, "")]
    if not nonempty:
        return ""
    first = nonempty[0]
    if all(v == first for v in nonempty):
        return first
    return ""


def _worst_health(values: list[Any]) -> str:
    best = ""
    best_rank = -1
    for raw in values:
        score = str(raw or "NONE").strip().upper() or "NONE"
        rank = _CSR_HEALTH_RANK.get(score, 0)
        if rank > best_rank:
            best = score
            best_rank = rank
    return best or "NONE"


def rollup_csr_site_rows(sites: list[dict[str, Any]], *, level: str) -> list[dict[str, Any]]:
    """Aggregate factory rows to BU or entity grain (internal keys)."""
    group_key = _CSR_ROLLUP_GROUP_KEY[level]
    drop = _CSR_ROLLUP_DROP[level]
    buckets: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for site in sites:
        raw = site.get(group_key)
        label = str(raw).strip() if raw not in (None, "") else "(blank)"
        if label not in buckets:
            buckets[label] = []
            order.append(label)
        buckets[label].append(site)

    rolled: list[dict[str, Any]] = []
    for label in order:
        members = buckets[label]
        row: dict[str, Any] = {
            group_key: "" if label == "(blank)" else label,
            "factory_count": len(members),
            "health_score": _worst_health([m.get("health_score") for m in members]),
        }
        keys = {k for m in members for k in m.keys()} - drop - {group_key, "health_score", "factory_count"}
        for key in sorted(keys):
            values = [m.get(key) for m in members]
            if key in _CSR_SUM_KEYS:
                nums = [_numeric(v) for v in values]
                present = [n for n in nums if n is not None]
                if present:
                    total = sum(present)
                    row[key] = int(round(total)) if key in {k for _, k in _CSR_KPI_INT_FIELDS} else round(total)
            elif key in _CSR_MEAN_KEYS:
                nums = [_numeric(v) for v in values]
                present = [n for n in nums if n is not None]
                if present:
                    row[key] = round(sum(present) / len(present), 2)
            elif key == "automated_health_scores":
                continue
            else:
                agreed = _consensus(values)
                if agreed not in (None, ""):
                    row[key] = agreed
        rolled.append(row)
    return rolled


def build_csr_dump_tables(
    *,
    csr_customer: str,
    folder: str,
    slot: str,
    export_date: dt.date,
    level: str,
    row_count: int,
    presented: list[dict[str, Any]],
    columns: list[str],
    source_meta: dict[str, str],
) -> dict[str, list[list[Any]]]:
    exported = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tab = _CSR_ROLLUP_TAB[level]
    meta_rows: list[list[Any]] = [
        ["field", "value"],
        ["csr_customer", csr_customer],
        ["customer_exports_folder", folder],
        ["grain", _CSR_LEVEL_LABEL[level]],
        ["delta", "week"],
        ["run_slot", slot],
        ["export_date_chicago", export_date.isoformat()],
        ["exported_utc", exported],
        ["row_count", row_count],
        ["source_file", source_meta.get("file") or ""],
        ["source_modified", source_meta.get("modified") or ""],
    ]
    if level != "site":
        meta_rows.append(["rollup_note", _CSR_ROLLUP_NOTE])
    grid: list[list[Any]] = [list(columns)]
    for row in presented:
        grid.append([_cell_value(row.get(col)) for col in columns])
    if len(grid) == 1:
        grid.append([""] * max(len(columns), 1) if columns else [""])
    return {"meta": meta_rows, tab: grid}


def _md_cell(value: Any) -> str:
    text = "" if value is None else str(_cell_value(value))
    return text.replace("|", "\\|").replace("\r", " ").replace("\n", " ").strip()


def _markdown_grain_table(columns: list[str], presented: list[dict[str, Any]]) -> list[str]:
    if not columns:
        return ["_No columns available in this CS Report export._"]
    header = f"| {' | '.join(_md_cell(c) for c in columns)} |"
    divider = f"|{'---|' * len(columns)}"
    lines = [header, divider]
    for row in presented:
        lines.append(f"| {' | '.join(_md_cell(row.get(c)) for c in columns)} |")
    if not presented:
        lines.append(f"|{' |' * len(columns)}")
        lines.append("")
        lines.append("_No rows at this grain for the latest CS Report week._")
    return lines


def render_csr_dump_markdown(
    *,
    csr_customer: str,
    folder: str,
    slot: str,
    export_date: dt.date,
    level: str,
    titles: dict[str, str],
    presented: list[dict[str, Any]],
    columns: list[str],
    site_count: int,
    bu_count: int,
    entity_count: int,
    source_meta: dict[str, str],
    spreadsheet_urls: dict[str, str] | None = None,
    mapped: bool = True,
) -> str:
    """Render the markdown twin of one grain's Sheet (full table, not a pointer)."""
    urls = spreadsheet_urls or {}
    grain = _CSR_LEVEL_LABEL[level]
    own_url = urls.get(level) or ""
    lines = [
        f"# {csr_customer} Customer Success Report — {grain} level",
        "",
        f"- **CSR customer:** {csr_customer}",
        f"- **Customer Exports folder:** `{CUSTOMER_EXPORTS_FOLDER}/{folder}/`",
        f"- **Grain:** {grain} ({len(presented)} row(s))",
        f"- **Delta:** week",
        f"- **Run slot:** `{slot}` (America/Chicago; not in the filename)",
        f"- **Chicago date:** {export_date.isoformat()} (`{csr_report_date_label(export_date)}`)",
        f"- **Exported (UTC):** {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"- **Sites:** {site_count} · **Business units:** {bu_count} · **Entities:** {entity_count}",
        f"- **Source workbook:** {source_meta.get('file') or '(unknown)'}",
        f"- **Source modified:** {source_meta.get('modified') or '(unknown)'}",
        f"- **Historical snapshot:** `Historical Data/{export_date.isoformat()}/{slot}/`",
    ]
    if own_url:
        lines.append(f"- **This grain as a Sheet:** {own_url}")
    if not mapped:
        lines.append(
            "- **Join note:** no Pendo/cohort/CS Report alias matched this CSR name; "
            "folder uses the workbook customer string. Salesforce remains system of record "
            "for commercial status — this dump is CS Report inventory only."
        )
    lines.extend(
        [
            "",
            "## Companion files",
            "",
            "| Grain | File | Sheet |",
            "|-------|------|-------|",
        ]
    )
    for other, label in (("site", "Site"), ("bu", "Business unit"), ("entity", "Entity")):
        title = titles[other]
        link = urls.get(other) or ""
        sheet = f"[open]({link})" if link else title
        marker = " (this file)" if other == level else ""
        lines.append(f"| {label}{marker} | `{title}` | {sheet} |")
    if level != "site":
        lines.extend(["", _CSR_ROLLUP_NOTE])
    lines.extend(
        [
            "",
            f"## {grain.title()} data (`delta=week`, CSR UI column labels)",
            "",
        ]
    )
    lines.extend(_markdown_grain_table(columns, presented))
    lines.append("")
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
    diag: ExportRunDiagnostics | None = None,
) -> dict[str, Any]:
    slot_label = historical_run_slot_label(slot)
    export_date = chicago_export_date(now)
    if diag is not None:
        with export_phase(diag, "csr-dump-load"):
            rows = load_latest_csr_week_rows()
    else:
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
            titles = {level: csr_dump_report_title(level, export_date) for level in ("site", "bu", "entity")}
            site_presented, site_cols = csr_sites_and_columns_for_export(sites)
            bu_rows = rollup_csr_site_rows(sites, level="bu")
            entity_rows = rollup_csr_site_rows(sites, level="entity")
            bu_presented, bu_cols = csr_sites_and_columns_for_export(bu_rows)
            entity_presented, entity_cols = csr_sites_and_columns_for_export(entity_rows)
            payloads = {
                "site": (site_presented, site_cols, len(sites)),
                "bu": (bu_presented, bu_cols, len(bu_rows)),
                "entity": (entity_presented, entity_cols, len(entity_rows)),
            }
            md_kwargs = dict(
                csr_customer=csr_name,
                folder=folder,
                slot=slot_label,
                export_date=export_date,
                titles=titles,
                site_count=len(sites),
                bu_count=len(bu_rows),
                entity_count=len(entity_rows),
                source_meta=source_meta,
                mapped=mapped,
            )

            def _render(level: str, urls: dict[str, str] | None = None) -> str:
                presented, columns, _count = payloads[level]
                return render_csr_dump_markdown(
                    level=level,
                    presented=presented,
                    columns=columns,
                    spreadsheet_urls=urls,
                    **md_kwargs,
                )

            if no_drive:
                from .export_customer_pendo_snapshot import _write_local

                written: list[str] = []
                for level in ("site", "bu", "entity"):
                    dest = local_dir / folder / slot_label / f"{titles[level]}.md"
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    _write_local(dest, _render(level))
                    written.append(str(dest))
                uploaded.append({"customer": csr_name, "folder": folder, "local_md": written})
                continue

            from .drive_config import upload_text_file_to_drive_folder
            from .export_drive_layout import ensure_customer_export_folders
            from .export_output_archive import maybe_migrate_export_layout_on_startup

            if not migrated:
                maybe_migrate_export_layout_on_startup()
                migrated = True
            folders = ensure_customer_export_folders(folder)
            urls: dict[str, str] = {}
            last_upload: dict[str, str] = {}
            for level in ("site", "bu", "entity"):
                presented, columns, row_count = payloads[level]
                tables = build_csr_dump_tables(
                    csr_customer=csr_name,
                    folder=folder,
                    slot=slot_label,
                    export_date=export_date,
                    level=level,
                    row_count=row_count,
                    presented=presented,
                    columns=columns,
                    source_meta=source_meta,
                )
                last_upload = upload_csr_markdown_and_spreadsheet(
                    title=titles[level],
                    md=_render(level),
                    tables=tables,
                    persistent_folder_id=folders["persistent_folder_id"],
                    historical_folder_id=folders["historical_folder_id"],
                    base_label=folders["base_label"],
                    slot=slot_label,
                    export_date=export_date,
                )
                urls[level] = last_upload.get("persistent_spreadsheet_url") or ""
            for level in ("site", "bu", "entity"):
                upload_text_file_to_drive_folder(
                    f"{titles[level]}.md",
                    _render(level, urls),
                    folders["persistent_folder_id"],
                    mime_type="text/markdown",
                )
            slot_folder_id = last_upload.get("historical_slot_folder_id")
            if slot_folder_id:
                for level in ("site", "bu", "entity"):
                    upload_text_file_to_drive_folder(
                        f"{titles[level]}.md",
                        _render(level, urls),
                        slot_folder_id,
                        mime_type="text/markdown",
                    )
            uploaded.append({"customer": csr_name, "folder": folder, "titles": titles, **urls})
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
    with export_diagnostics_scope() as diag:
        export_csr_dumps(
            slot=slot,
            customer=args.customer,
            no_drive=args.no_drive,
            out_dir=args.out_dir,
            diag=diag,
        )


if __name__ == "__main__":
    export_csr_main(sys.argv[1:])
