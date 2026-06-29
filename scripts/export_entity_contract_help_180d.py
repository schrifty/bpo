"""Export Salesforce Customer Entity rows with HELP ticket counts by day-range buckets after factory start.

Buckets are **inclusive** day offsets from ``factory_start_date`` (0 = factory start calendar day):

  - days 0–40
  - days 41–80
  - days 81–120
  - days 121–160
  - days 161–200

Each bucket counts issues with ``created`` in ``[start + lo, start + hi + 1)`` (half-open on the end date).

Uses ``SF_ACCOUNT_FACTORY_START_DATE_FIELD`` (default ``Effective_Date_of_Order__c``).
Counts **project HELP** with **JSM Organizations + summary/description site phrases**
(see ``JiraClient.help_salesforce_entity_site_scoped_clause``) so broad orgs (e.g. Carrier)
are not fully duplicated across every entity row. Excludes Outage/Healthcheck labels
(``_TRANSIENT_LABELS_EXCLUSION``). Rows that fall back to org-only scope include
``help_scope_org_only`` in ``note``.

Usage (from repo root):
  python scripts/export_entity_contract_help_180d.py
  python scripts/export_entity_contract_help_180d.py -o out.tsv --workers 3

Requires ``.env`` with Salesforce + JIRA_* credentials.
"""
from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.config import SF_ACCOUNT_FACTORY_START_DATE_FIELD
from src.jira_client import (
    HELP_FACTORY_START_DAY_BUCKETS,
    _TRANSIENT_LABELS_EXCLUSION,
    _salesforce_entity_customer_primary_and_extras,
    get_shared_jira_client,
)
from src.salesforce_client import SalesforceClient, _parse_sf_contract_date

HELP_TICKET_DAY_BUCKETS: tuple[tuple[int, int, str], ...] = tuple(
    (lo, hi, key) for lo, hi, key, _lbl in HELP_FACTORY_START_DAY_BUCKETS
)


def _help_bucket_counts_for_row(jira, row: dict) -> tuple[list[int | None], str]:
    """Return (one count per HELP_TICKET_DAY_BUCKET, note)."""
    start = _parse_sf_contract_date(row.get("factory_start_date"))
    if not start:
        return [None] * len(HELP_TICKET_DAY_BUCKETS), "no_factory_start_date"

    primary, _extras = _salesforce_entity_customer_primary_and_extras(row)
    if not primary:
        return [None] * len(HELP_TICKET_DAY_BUCKETS), "no_entity_name"

    scope_clause, scope_meta = jira.help_salesforce_entity_site_scoped_clause(row)
    if "___CORTEX_NO_ORG_MATCH___" in scope_clause:
        return [None] * len(HELP_TICKET_DAY_BUCKETS), "no_jsm_org_match"

    counts: list[int | None] = []
    failed = False
    for lo, hi, _col in HELP_TICKET_DAY_BUCKETS:
        d0 = start + timedelta(days=lo)
        d1 = start + timedelta(days=hi + 1)
        jql = (
            f"project = HELP AND {scope_clause} AND {_TRANSIENT_LABELS_EXCLUSION} "
            f'AND created >= "{d0:%Y-%m-%d}" AND created < "{d1:%Y-%m-%d}"'
        )
        total = jira._jql_match_total(jql)
        if total is None:
            failed = True
            counts.append(None)
        else:
            counts.append(total)

    notes: list[str] = []
    if failed:
        notes.append("jira_count_failed")
    if not scope_meta.get("site_text_scoped"):
        notes.append("help_scope_org_only")
    note = ";".join(notes)
    return counts, note


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Entity factory start + HELP ticket counts in day buckets "
            "(0–40, 41–80, 81–120, 121–160, 161–200) after factory start."
        ),
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("entity_factory_start_help_day_buckets.tsv"),
        help="Output TSV path (default: entity_factory_start_help_day_buckets.tsv)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Parallel entity workers (each entity runs 5 Jira counts; default: 3)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N entities (0 = all)",
    )
    args = ap.parse_args()

    sf_rows = SalesforceClient().get_entity_accounts()
    sf_rows.sort(key=lambda x: ((x.get("Name") or "").strip().lower()))

    if args.limit and args.limit > 0:
        sf_rows = sf_rows[: args.limit]

    try:
        jira = get_shared_jira_client()
    except Exception as e:
        print(f"Jira client unavailable: {e}", file=sys.stderr)
        sys.exit(1)

    workers = max(1, args.workers)

    def job(row: dict) -> tuple[str, list[int | None], str]:
        cnts, note = _help_bucket_counts_for_row(jira, row)
        rid = str(row.get("Id") or "")
        return rid, cnts, note

    by_id: dict[str, tuple[list[int | None], str]] = {}
    if workers == 1:
        for row in sf_rows:
            rid, cnts, note = job(row)
            by_id[rid] = (cnts, note)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(job, row) for row in sf_rows]
            for fut in as_completed(futs):
                rid, cnts, note = fut.result()
                by_id[rid] = (cnts, note)

    fs_api = SF_ACCOUNT_FACTORY_START_DATE_FIELD
    bucket_headers = "\t".join(col for _lo, _hi, col in HELP_TICKET_DAY_BUCKETS)
    header = (
        "Name\tfactory_start_date\tfactory_start_date_field\tContract_Contract_Start_Date__c\t"
        f"LeanDNA_Entity_Name__c\t{bucket_headers}\tnote\n"
    )
    lines = [header]
    for row in sf_rows:
        rid = str(row.get("Id") or "")
        cnts, note = by_id.get(rid, ([None] * len(HELP_TICKET_DAY_BUCKETS), "missing_result"))
        fs_val = row.get("factory_start_date")
        fs_display = "" if fs_val is None else str(fs_val).strip()
        count_cells = ["" if c is None else str(c) for c in cnts]
        lines.append(
            "\t".join(
                [
                    (row.get("Name") or "").strip(),
                    fs_display,
                    fs_api,
                    str(row.get("Contract_Contract_Start_Date__c") or "").strip(),
                    (row.get("LeanDNA_Entity_Name__c") or "").strip(),
                    *count_cells,
                    note,
                ]
            )
        )

    args.output.write_text("\n".join(lines), encoding="utf-8")
    print(
        f"{len(sf_rows)} rows -> {args.output.resolve()} "
        f"(factory_start_field={fs_api}, buckets=0-40,41-80,81-120,121-160,161-200)"
    )


if __name__ == "__main__":
    main()
