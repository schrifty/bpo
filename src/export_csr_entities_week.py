"""Portfolio CS Report entity-grain export (uncapped JSON) for Output/.

One row per ``(csr_customer, entity)`` from ``delta=week`` factory rows, using the same
Cortex entity rollup as the per-customer dump (sums / unweighted means — not yet validated
against a native APEX entity download).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

from .config import logger
from .cs_report_client import (
    CSR_MERGED_SITE_EXPORT_COLUMNS,
    csr_export_column_label,
    csr_latest_report_meta,
    csr_site_entries_for_exact_week_customer,
    distinct_csr_week_customers,
    load_latest_csr_week_rows,
    present_csr_site_for_export,
)
from .export_csr_dump import (
    chicago_export_date,
    customer_exports_folder_for_csr_name,
    rollup_csr_site_rows,
)
from .export_drive_layout import (
    ensure_portfolio_output_folders,
    upload_text_persistent_and_historical,
)
from .export_run_diagnostics import export_diagnostics_scope, export_phase

CSR_ENTITIES_WEEK_STEM = "csr-entities-week"
CSR_ENTITIES_WEEK_NOTE = (
    "Entity grain from CS Report delta=week factory rows: counts and dollar KPIs are summed "
    "across sites; percents, DOI, and similar rates are unweighted means of sites with a value. "
    "This is a Cortex rollup, not a native LeanDNA APEX entity extract, until that download is "
    "compared. Salesforce remains system of record for commercial status."
)
_IDENTITY_COLUMNS = ("csr_customer", "customer_exports_folder", "Site count")


def csr_entities_week_columns() -> list[str]:
    """Stable all-column header: identity + every CSR export label (empty cells allowed)."""
    labels = [csr_export_column_label(k) for k in CSR_MERGED_SITE_EXPORT_COLUMNS]
    seen = set(_IDENTITY_COLUMNS)
    out = list(_IDENTITY_COLUMNS)
    for lab in labels:
        if lab not in seen:
            seen.add(lab)
            out.append(lab)
    return out


def build_csr_entities_week_rows(week_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Uncapped entity rows for every CS Report week customer (workbook inventory)."""
    customers = distinct_csr_week_customers(week_rows)
    combined: list[dict[str, Any]] = []
    for csr_name in customers:
        folder = customer_exports_folder_for_csr_name(csr_name)
        sites = csr_site_entries_for_exact_week_customer(csr_name, week_rows)
        for rolled in rollup_csr_site_rows(sites, level="entity"):
            presented = present_csr_site_for_export(rolled)
            site_count = rolled.get("factory_count")
            if site_count is not None:
                presented["Site count"] = site_count
            presented["csr_customer"] = csr_name
            presented["customer_exports_folder"] = folder
            combined.append(presented)
    entity_label = csr_export_column_label("entity")
    combined.sort(
        key=lambda r: (
            str(r.get("csr_customer") or "").lower(),
            str(r.get(entity_label) or "").lower(),
        )
    )
    return combined


def align_csr_entities_week_rows(
    rows: list[dict[str, Any]],
    columns: list[str],
) -> list[dict[str, Any]]:
    return [{col: row.get(col) for col in columns} for row in rows]


def build_csr_entities_week_document(
    week_rows: list[dict[str, Any]],
    *,
    exported_at_utc: str | None = None,
    source_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    columns = csr_entities_week_columns()
    raw_rows = build_csr_entities_week_rows(week_rows)
    rows = align_csr_entities_week_rows(raw_rows, columns)
    meta = source_meta if isinstance(source_meta, dict) else (csr_latest_report_meta() or {})
    return {
        "grain": "entity",
        "delta": "week",
        "uncapped": True,
        "note": CSR_ENTITIES_WEEK_NOTE,
        "source": {
            "file": meta.get("file"),
            "modified": meta.get("modified"),
        },
        "exported_at_utc": exported_at_utc
        or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
    }


def render_csr_entities_week_json(doc: dict[str, Any]) -> str:
    return json.dumps(doc, indent=2, ensure_ascii=False, default=str) + "\n"


def write_csr_entities_week_local(doc: dict[str, Any], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{CSR_ENTITIES_WEEK_STEM}.json"
    json_path.write_text(render_csr_entities_week_json(doc), encoding="utf-8")
    return {"json": str(json_path)}


def upload_csr_entities_week_document(doc: dict[str, Any]) -> dict[str, str]:
    folders = ensure_portfolio_output_folders()
    json_urls = upload_text_persistent_and_historical(
        stem=CSR_ENTITIES_WEEK_STEM,
        content=render_csr_entities_week_json(doc),
        ext=".json",
        persistent_folder_id=folders["persistent_folder_id"],
        historical_folder_id=folders["historical_folder_id"],
        base_label=folders["base_label"],
        mime_type="application/json",
        mirror_layouts=folders.get("mirror_layouts"),
    )
    return {
        "persistent_json_id": json_urls["persistent_file_id"],
        "historical_json_id": json_urls["historical_file_id"],
        "row_count": str(doc.get("row_count") or 0),
    }


def export_csr_entities_week_from_rows(
    week_rows: list[dict[str, Any]],
    *,
    no_drive: bool = False,
    out_dir: Path | None = None,
) -> dict[str, Any]:
    """Build the entity-week document and write Drive Output/ (or a local file)."""
    if not distinct_csr_week_customers(week_rows):
        raise RuntimeError("CS Report has no delta=week rows — cannot build entity-week export")
    doc = build_csr_entities_week_document(week_rows)
    if no_drive:
        paths = write_csr_entities_week_local(
            doc,
            out_dir or Path("output") / "csr-entities-week",
        )
        logger.info(
            "CSR entity-week export wrote %d row(s) locally json=%s",
            doc["row_count"],
            paths["json"],
        )
        return {"row_count": doc["row_count"], "local": paths, "skipped_drive": True}
    urls = upload_csr_entities_week_document(doc)
    logger.info(
        "CSR entity-week export uploaded %d row(s) → Output/%s-persistent.json",
        doc["row_count"],
        CSR_ENTITIES_WEEK_STEM,
    )
    print(
        f"Uploaded {doc['row_count']} entity row(s) → Output/{CSR_ENTITIES_WEEK_STEM}-persistent.json"
    )
    print(
        f"Output/ (JSON): https://drive.google.com/file/d/{urls['persistent_json_id']}/view"
    )
    return {"row_count": doc["row_count"], "drive": urls}


def export_csr_entities_week_main(
    argv: list[str] | None = None,
    *,
    prog: str = "cortex --export-csr-entities",
) -> None:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("--no-drive", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    with export_diagnostics_scope() as diag:
        with export_phase(diag, "csr-entities-week"):
            rows = load_latest_csr_week_rows()
            if not rows:
                raise RuntimeError(
                    "CS Report workbook missing or empty — cannot build entity-week export (fail loud)"
                )
            export_csr_entities_week_from_rows(
                rows, no_drive=args.no_drive, out_dir=args.out_dir
            )
        job_name = os.environ.get("CORTEX_JOB_NAME", "").strip() or "export-csr-entities"
        diag.emit_run_summary(job_name=job_name, fail_on_warnings=False)


if __name__ == "__main__":
    export_csr_entities_week_main(sys.argv[1:])
