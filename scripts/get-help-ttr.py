#!/usr/bin/env python3
"""HELP TTR SLA adherence % (trailing window) vs LeanDNA metric 1911-style stored values.

**New (Jira):** among HELP tickets **resolved** in the last N days (default 30), the percent
with **Time to resolution** SLA completed and **not breached** (``customfield_10665``).

**Old (LeanDNA):** default catalog metric **1911** — *TTR % (Trailing 30 Days)* /
*Support Time to Resolution* — ``MetricDataPoint`` series over the same window.

Requires ``JIRA_*`` and LeanDNA Data API credentials in ``.env``.

Examples::

  get-help-ttr
  get-help-ttr --days 30 --format json
  get-help-ttr --metric-id 1911 --requested-sites 416
  get-help-ttr --customer Carrier --include-tickets
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.cli_warning_filters import apply_cli_warning_filters  # noqa: E402

apply_cli_warning_filters()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

import requests  # noqa: E402

from src.config import BPO_LEANDNA_DATA_API_EXECUTION_BUCKET  # noqa: E402
from src.jira_client import get_shared_jira_client  # noqa: E402
from src.leandna_data_api_http import leandna_data_api_credentials_configured  # noqa: E402
from src.leandna_data_api_request import data_api_base_url  # noqa: E402
from src.leandna_metrics_client import (  # noqa: E402
    fetch_metric_datapoints,
    find_similar_metric_definitions,
    list_metric_definitions,
    metric_definition_label,
    resolve_metric_datapoint_window,
    slim_metric_datapoint_rows,
    summarize_metric_datapoint_values,
)

# LeanDNA prod catalog: TTR % (Trailing 30 Days) / Support Time to Resolution
DEFAULT_LEANDNA_METRIC_ID = "1911"
DEFAULT_METRIC_SEARCH = "Support Time to Resolution"


def _build_comparison(jira: dict[str, Any] | None, leandna: dict[str, Any] | None) -> dict[str, Any]:
    """Compare adherence % (Jira recomputed vs LeanDNA stored latest / median in window)."""
    cmp_out: dict[str, Any] = {}
    jira_adh = (jira or {}).get("ttr_sla_adherence") or {}
    jira_pct = jira_adh.get("pct")
    lean_sum = (leandna or {}).get("summary") or {}
    lean_latest = lean_sum.get("latest")
    lean_median = lean_sum.get("median")

    if jira_pct is not None and lean_latest is not None:
        cmp_out["latest"] = {
            "old_leandna_pct": lean_latest,
            "old_leandna_date": lean_sum.get("latest_date"),
            "new_jira_pct": jira_pct,
            "delta_pct_points": round(float(jira_pct) - float(lean_latest), 1),
        }
    if jira_pct is not None and lean_median is not None:
        cmp_out["median_in_window"] = {
            "old_leandna_median_pct": lean_median,
            "new_jira_pct": jira_pct,
            "delta_pct_points": round(float(jira_pct) - float(lean_median), 1),
        }
    return cmp_out


def _fetch_leandna_metric_block(
    *,
    days: int,
    metric_search: str,
    metric_id: str | None,
    requested_sites: str | None,
    connect_timeout: float,
    read_timeout: float,
) -> dict[str, Any]:
    if not leandna_data_api_credentials_configured():
        return {
            "error": "Missing LeanDNA Data API credentials (LEANDNA_DATA_API_BEARER_TOKEN / COOKIE).",
        }

    catalog = list_metric_definitions(
        requested_sites=requested_sites,
        connect_timeout_seconds=connect_timeout,
        timeout_seconds=read_timeout,
    )
    start_s, end_s = resolve_metric_datapoint_window(lookback_days=days)

    if metric_id:
        want = metric_id.strip()
        matched = next(
            (m for m in catalog if str(m.get("id")).strip() == want),
            None,
        )
        candidates: list[dict[str, Any]] = []
        if matched is None:
            return {
                "error": f"No metric with id={want!r} in catalog ({len(catalog)} definitions).",
                "search_term": metric_search,
                "data_window": {"startDate": start_s, "endDate": end_s},
            }
        matched = dict(matched)
        matched["match_score"] = 1.0
    else:
        candidates = find_similar_metric_definitions(
            catalog,
            metric_search,
            window_days=days,
        )
        matched = candidates[0] if candidates else None

    if matched is None:
        return {
            "error": f"No LeanDNA metric matched {metric_search!r} (catalog size {len(catalog)}).",
            "search_term": metric_search,
            "candidates": [],
            "data_window": {"startDate": start_s, "endDate": end_s},
        }

    mid = matched.get("id")
    sites = requested_sites
    if not sites and matched.get("siteId") is not None:
        sites = str(matched.get("siteId")).strip() or None

    points, err = fetch_metric_datapoints(
        mid,
        start_date=start_s,
        end_date=end_s,
        requested_sites=sites,
        timeout_seconds=read_timeout,
    )
    slim = slim_metric_datapoint_rows(points)
    summary = summarize_metric_datapoint_values(slim)

    block: dict[str, Any] = {
        "source": "leandna",
        "role": "old_stored_metric",
        "search_term": metric_search,
        "metric": {
            "id": mid,
            "name": metric_definition_label(matched),
            "crossSiteName": matched.get("crossSiteName"),
            "siteId": matched.get("siteId"),
            "match_score": matched.get("match_score"),
            "ownerId": matched.get("ownerId"),
        },
        "candidates": [
            {
                "id": c.get("id"),
                "name": metric_definition_label(c),
                "match_score": c.get("match_score"),
            }
            for c in (candidates[:5] if not metric_id else [])
        ],
        "data_window": {"startDate": start_s, "endDate": end_s},
        "dataSeries": slim,
        "summary": summary,
    }
    if err is not None:
        block["dataSeriesError"] = {"status": err.get("status"), "error": err.get("error")}
    return block


def _print_brief(payload: dict[str, Any]) -> None:
    days = payload.get("window_days", 30)
    print(f"TTR SLA adherence % — HELP resolved tickets — trailing {days}d")
    print("(LeanDNA metric 1911: TTR % (Trailing 30 Days) / Support Time to Resolution)")
    print()

    lean = payload.get("leandna") or {}
    if lean.get("error"):
        print(f"LeanDNA (stored): error — {lean['error']}")
    elif lean:
        m = lean.get("metric") or {}
        print(
            f"LeanDNA (stored): {m.get('name')} (id={m.get('id')}, "
            f"match={m.get('match_score')})"
        )
        summ = lean.get("summary") or {}
        if summ.get("measured"):
            print(
                f"  Points: {summ.get('points')}  "
                f"Latest: {summ.get('latest')}% ({summ.get('latest_date')})  "
                f"Median: {summ.get('median'):.2g}%  Avg: {summ.get('avg'):.2g}%"
            )
        else:
            print("  No numeric datapoints in window.")
        alts = lean.get("candidates") or []
        if len(alts) > 1:
            print("  Other matches:", ", ".join(f"{c['name']} ({c['match_score']})" for c in alts[1:4]))
    print()

    jira = payload.get("jira") or {}
    if jira.get("error"):
        print(f"Jira HELP (recomputed): error — {jira['error']}")
    elif jira:
        customer = jira.get("customer")
        scope = f"customer {customer!r}" if customer else "portfolio"
        adh = jira.get("ttr_sla_adherence") or {}
        print(f"Jira HELP (recomputed): {scope}")
        print(f"  Resolved in window: {jira.get('resolved_in_window')}")
        if adh.get("pct") is not None:
            print(
                f"  TTR SLA adherence: {adh.get('pct')}%  "
                f"({adh.get('met', 0)} met / {adh.get('measured', 0)} with completed TTR SLA, "
                f"{adh.get('breached', 0)} breached)"
            )
        else:
            print("  TTR SLA adherence: — (no tickets with completed TTR SLA in fetch)")
        if adh.get("waiting"):
            print(f"  TTR SLA still in progress (excluded from %): {adh.get('waiting')}")

    cmp_block = payload.get("comparison") or {}
    if cmp_block:
        print()
        print("Comparison (stored LeanDNA % → Jira recomputed %):")
        lat = cmp_block.get("latest") or {}
        if lat:
            print(
                f"  Latest stored {lat.get('old_leandna_pct')}% ({lat.get('old_leandna_date')}) "
                f"vs Jira {lat.get('new_jira_pct')}%  "
                f"(delta {lat.get('delta_pct_points'):+.1f} pts)"
            )
        med = cmp_block.get("median_in_window") or {}
        if med:
            print(
                f"  Median stored {med.get('old_leandna_median_pct')}% "
                f"vs Jira {med.get('new_jira_pct')}%  "
                f"(delta {med.get('delta_pct_points'):+.1f} pts)"
            )

    tickets = jira.get("tickets") if isinstance(jira, dict) else None
    if tickets:
        print()
        print("key\tresolved\tTTR_SLA_measured\tTTR_SLA_met\torganizations\tsummary")
        for t in tickets:
            key = t.get("key", "")
            rd = t.get("resolutiondate", "")
            meas = "yes" if t.get("ttr_sla_measured") else "no"
            met = "yes" if t.get("ttr_sla_met") else ("no" if t.get("ttr_sla_measured") else "")
            orgs_s = ",".join(t.get("organizations") or [])[:60]
            summary = str(t.get("summary") or "").replace("\t", " ").replace("\n", " ")[:80]
            print(f"{key}\t{rd}\t{meas}\t{met}\t{orgs_s}\t{summary}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Compare HELP TTR SLA adherence % (trailing window) with LeanDNA metric "
            "1911 (TTR % Trailing 30 Days)."
        ),
    )
    ap.add_argument("--days", type=int, default=30, metavar="N", help="Trailing window (default: 30)")
    ap.add_argument("--customer", default=None, metavar="NAME", help="HELP customer / JSM org scope")
    ap.add_argument("--match-term", action="append", default=[], metavar="TEXT")
    ap.add_argument(
        "--metric-search",
        default=DEFAULT_METRIC_SEARCH,
        help=f"LeanDNA catalog search if --metric-id omitted (default: {DEFAULT_METRIC_SEARCH!r})",
    )
    ap.add_argument(
        "--metric-id",
        default=DEFAULT_LEANDNA_METRIC_ID,
        metavar="ID",
        help=f"LeanDNA metric id (default: {DEFAULT_LEANDNA_METRIC_ID})",
    )
    ap.add_argument("--requested-sites", default=None, metavar="ID", help="LeanDNA RequestedSites header")
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ap.add_argument("--include-tickets", action="store_true", help="Include per-issue HELP rows")
    ap.add_argument("--skip-jira", action="store_true", help="LeanDNA metric only")
    ap.add_argument("--skip-metrics", action="store_true", help="Jira HELP only")
    ap.add_argument("--max-results", type=int, default=None, metavar="N", help="Cap HELP issues fetched")
    ap.add_argument("--connect-timeout", type=float, default=15.0, metavar="SEC")
    ap.add_argument("--timeout", type=float, default=120.0, dest="read_timeout", metavar="SEC")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args()

    bpo_log = logging.getLogger("bpo")
    bpo_log.setLevel(logging.INFO if ns.verbose else logging.WARNING)
    bpo_log.propagate = False

    payload: dict[str, Any] = {"window_days": ns.days}
    exit_code = 0

    if not ns.skip_metrics:
        try:
            if leandna_data_api_credentials_configured():
                base = data_api_base_url()
                print(
                    f"LeanDNA: GET {base}/data/Metric + MetricDataPoint  "
                    f"(bucket: {BPO_LEANDNA_DATA_API_EXECUTION_BUCKET})",
                    file=sys.stderr,
                )
            payload["leandna"] = _fetch_leandna_metric_block(
                days=ns.days,
                metric_search=ns.metric_search,
                metric_id=(str(ns.metric_id).strip() if ns.metric_id else None),
                requested_sites=ns.requested_sites,
                connect_timeout=ns.connect_timeout,
                read_timeout=ns.read_timeout,
            )
            if payload["leandna"].get("error"):
                print(payload["leandna"]["error"], file=sys.stderr)
                exit_code = 1
        except requests.Timeout as e:
            payload["leandna"] = {"error": f"LeanDNA request timed out: {e}"}
            print(payload["leandna"]["error"], file=sys.stderr)
            exit_code = 1
        except Exception as e:
            payload["leandna"] = {"error": str(e)}
            print(f"LeanDNA fetch failed: {e}", file=sys.stderr)
            exit_code = 1

    if not ns.skip_jira:
        try:
            jira = get_shared_jira_client()
        except ValueError as e:
            payload["jira"] = {"error": str(e)}
            print(str(e), file=sys.stderr)
            exit_code = 1
        else:
            match_terms = [t.strip() for t in ns.match_term if t and str(t).strip()]
            customer = (str(ns.customer).strip() if ns.customer is not None else "") or None
            try:
                jira_result = jira.get_help_time_to_resolution(
                    days=ns.days,
                    customer_name=customer,
                    match_terms=match_terms or None,
                    max_results=ns.max_results,
                    include_tickets=ns.include_tickets,
                )
                jira_result["role"] = "new_jira_recomputed"
                jira_result["source"] = "jira"
                payload["jira"] = jira_result
                if jira_result.get("error"):
                    print(jira_result["error"], file=sys.stderr)
                    exit_code = 1
            except Exception as e:
                payload["jira"] = {"error": str(e)}
                print(f"Jira fetch failed: {e}", file=sys.stderr)
                exit_code = 1

    payload["comparison"] = _build_comparison(
        payload.get("jira") if isinstance(payload.get("jira"), dict) else None,
        payload.get("leandna") if isinstance(payload.get("leandna"), dict) else None,
    )

    if ns.format == "json":
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
    else:
        _print_brief(payload)

    if ns.skip_jira and ns.skip_metrics:
        print("Nothing to do: remove --skip-jira and/or --skip-metrics.", file=sys.stderr)
        return 1

    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
