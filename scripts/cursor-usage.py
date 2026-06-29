#!/usr/bin/env python3
"""Pull Cursor Team Admin API usage data (AI coding adoption / spend / tokens).

Requires ``CURSOR_ADMIN_API_KEY`` in ``.env`` (Cursor dashboard → Settings).

Examples::

  cursor-usage                      # 30-day usage summary
  cursor-usage --days 7
  cursor-usage --report members
  cursor-usage --report spend --format json
  cursor-usage --report events --days 3 --email dev@company.com
"""
from __future__ import annotations

import argparse
import json
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

from src.cursor_client import (  # noqa: E402
    CursorClient,
    CursorClientError,
    cursor_configured,
)


def _print_summary(client: CursorClient, days: int) -> None:
    s = client.get_usage_summary(days=days)
    rate = s.acceptance_rate
    rate_s = f"{rate * 100:.1f}%" if rate is not None else "—"
    print(f"Cursor usage summary — {s.start_date} → {s.end_date} ({s.days}d)")
    print(f"  Active users:          {s.active_users}")
    print(f"  Lines added (total):   {s.total_lines_added:,}")
    print(f"  Lines added (accepted):{s.accepted_lines_added:,}")
    print(f"  Accepts / Rejects:     {s.total_accepts:,} / {s.total_rejects:,}  (accept rate {rate_s})")
    print(f"  Tabs shown / accepted: {s.total_tabs_shown:,} / {s.total_tabs_accepted:,}")
    print(f"  Agent requests:        {s.agent_requests:,}")
    print(f"  Chat requests:         {s.chat_requests:,}")
    print(f"  Composer requests:     {s.composer_requests:,}")
    print(f"  Cmd+K usages:          {s.cmdk_usages:,}")
    print(f"  Bugbot usages:         {s.bugbot_usages:,}")


def _print_members(client: CursorClient) -> None:
    members = client.get_team_members()
    print(f"Cursor team members: {len(members)}")
    for m in members:
        print(f"  {m.get('email','')}\t{m.get('role','')}\t{m.get('name','')}")


def _print_spend(client: CursorClient) -> None:
    rows = client.get_spend()
    total = sum(float(r.get("overallSpendCents") or 0) for r in rows)
    print(f"Cursor spend (current billing cycle) — {len(rows)} member(s), "
          f"total ${total / 100:,.2f}")
    rows.sort(key=lambda r: float(r.get("overallSpendCents") or 0), reverse=True)
    for r in rows:
        spend = float(r.get("overallSpendCents") or 0) / 100
        print(f"  {r.get('email',''):<40} ${spend:>10,.2f}  "
              f"({r.get('fastPremiumRequests', 0)} premium req)")


def _print_events(client: CursorClient, days: int, email: str | None) -> None:
    from datetime import datetime, timedelta, timezone

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=max(1, days))
    events = client.get_usage_events(start, end, email=email)
    in_tok = sum(int((e.get("tokenUsage") or {}).get("inputTokens") or 0) for e in events)
    out_tok = sum(int((e.get("tokenUsage") or {}).get("outputTokens") or 0) for e in events)
    charged = sum(float(e.get("chargedCents") or 0) for e in events)
    print(f"Cursor usage events — last {days}d{f' for {email}' if email else ''}: {len(events)} event(s)")
    print(f"  Input tokens:  {in_tok:,}")
    print(f"  Output tokens: {out_tok:,}")
    print(f"  Charged:       ${charged / 100:,.2f}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Pull Cursor Team Admin API usage data.")
    ap.add_argument(
        "--report",
        choices=("summary", "members", "spend", "events"),
        default="summary",
        help="Which dataset to pull (default: summary)",
    )
    ap.add_argument("--days", type=int, default=30, metavar="N", help="Trailing window (default: 30)")
    ap.add_argument("--email", default=None, help="Filter usage events by user email")
    ap.add_argument("--format", choices=("brief", "json"), default="brief")
    ns = ap.parse_args()

    if not cursor_configured():
        print(
            "CURSOR_ADMIN_API_KEY not set. Add it to .env "
            "(Cursor dashboard → Settings → Cursor Admin API key).",
            file=sys.stderr,
        )
        return 1

    try:
        client = CursorClient()
        if ns.format == "json":
            payload = _json_payload(client, ns)
            print(json.dumps(payload, indent=2, default=str))
            return 0
        if ns.report == "summary":
            _print_summary(client, ns.days)
        elif ns.report == "members":
            _print_members(client)
        elif ns.report == "spend":
            _print_spend(client)
        elif ns.report == "events":
            _print_events(client, ns.days, ns.email)
    except CursorClientError as e:
        print(f"Cursor API error: {e}", file=sys.stderr)
        return 1
    return 0


def _json_payload(client: CursorClient, ns: argparse.Namespace) -> Any:
    if ns.report == "summary":
        return client.get_usage_summary(days=ns.days).__dict__
    if ns.report == "members":
        return client.get_team_members()
    if ns.report == "spend":
        return client.get_spend()
    if ns.report == "events":
        from datetime import datetime, timedelta, timezone

        end = datetime.now(timezone.utc)
        start = end - timedelta(days=max(1, ns.days))
        return client.get_usage_events(start, end, email=ns.email)
    return {}


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
