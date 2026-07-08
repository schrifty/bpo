#!/usr/bin/env python3
"""CLI wrapper around the ``cortex_meta`` collector (see ``src/cortex_meta_report.py``).

Prints the meta blob Cortex assembles about itself, plus a scannable headline. This is
the same data the ``cortex_showcase`` deck renders. Nothing here hits the network unless
``--live`` is passed.

Run:  python scripts/cortex_meta_collect.py [--export GLOB ...] [--live] [--days N]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Best-effort .env load so `--live` picks up creds the same way a normal run does.
try:  # pragma: no cover - convenience only
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except Exception:
    pass

from src.cortex_meta_report import build_cortex_meta_report  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--export", action="append", default=[],
        help="Glob(s) to local export .md artifacts to token-count (repeatable).",
    )
    ap.add_argument(
        "--live", action="store_true",
        help="Also pull live volume counts (Salesforce/Jira/GitHub/Cursor). Needs creds + network.",
    )
    ap.add_argument("--days", type=int, default=30, help="Window for live counts (default 30).")
    args = ap.parse_args()

    meta = build_cortex_meta_report(
        days=args.days,
        export_globs=args.export or None,
        live=args.live,
    )
    print(json.dumps(meta, indent=2))

    gb = meta["graph_breadth"]
    os_ = meta["output_surface"]
    print("\n=== Headline numbers we could actually show ===")
    print(f"- {gb['data_elements']} data elements ({gb['aliases_terms']} aliases/terms) "
          f"across {gb['source_systems']} source systems")
    print(f"- 1 system of record ({', '.join(gb['system_of_record'])}); "
          f"{len(gb['enrichment_sources'])} enrichment sources")
    print(f"- {gb['documented_api_endpoints']} documented API endpoints; "
          f"{gb['report_blobs_mapped']} report blobs mapped to sources")
    print(f"- {os_['slide_builders']} slide builders across {os_['slide_builder_modules']} modules; "
          f"{len(os_['portfolio_deck_types'])} portfolio deck types")
    print(f"- {meta['governance_assets']['config_yaml_files']} config/knowledge YAMLs; "
          f"{meta['governance_assets']['governance_docs']} data-governance docs")
    for e in meta["export_economics"]:
        print(f"- export '{e['artifact']}': {e['tokens']:,} tokens "
              f"({e['pct_of_budget']}% of {e['token_budget']:,} budget), {e['sections']} sections")

    lv = meta.get("live_volume")
    if lv:
        print(f"\n=== Live volume counts (last {lv['window_days']}d) ===")

        def _line(label: str, block: dict, fields: list[tuple[str, str]]) -> None:
            if "unavailable" in block:
                print(f"- {label}: unavailable — {block['unavailable']}")
                return
            parts = [f"{block.get(k)!s} {name}" for k, name in fields if block.get(k) is not None]
            print(f"- {label}: {', '.join(parts) if parts else '(no counts)'}")

        _line("Salesforce", lv["salesforce"], [("portfolio_customers", "portfolio customers")])
        _line("Jira (engineering)", lv["jira_engineering"], [
            ("in_flight_tickets", "in-flight"), ("closed_tickets_window", "closed"),
            ("open_bugs", "open bugs"), ("blockers_criticals", "blockers/criticals"),
            ("contributors", "contributors"),
        ])
        _line("GitHub", lv["github"], [
            ("repos", "repos"), ("commits_window", "commits"),
            ("merged_prs_window", "merged PRs"), ("contributors", "contributors"),
        ])
        _line("Cursor", lv["cursor"], [
            ("seats", "seats"), ("active_users_window", "active"),
            ("total_tokens_window", "tokens"), ("spend_usd_cycle", "USD spend (cycle)"),
        ])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
