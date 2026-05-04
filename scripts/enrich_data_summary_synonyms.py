#!/usr/bin/env python3
"""LLM-enrich ``config/data_summary.json`` with manufacturing-context synonyms (``terms``).

For each catalog ``path``, calls the fast LLM to propose up to **5** short slide phrases
synonymous with the metric, grounded in **discrete manufacturing**: factories, MRP/ERP,
buyers/planners, inventory, POs, shortages, shop floor usage — not generic SaaS marketing.

Skips paths that are too abstract or non-phrasable (e.g. ``days``, raw Ids, internal keys);
skipped paths are **logged to stderr** for follow-up catalog fixes.

Default: dry-run (prints skipped + suggestions, does not write). Use ``--apply`` to update JSON.

Loads ``<repo>/.env`` at startup (same keys as the app: ``OPENAI_API_KEY`` / ``GEMINI_API_KEY``, ``LLM_PROVIDER``).

Usage:
  python scripts/enrich_data_summary_synonyms.py
  python scripts/enrich_data_summary_synonyms.py --max-paths 20
  python scripts/enrich_data_summary_synonyms.py --apply --batch-size 8
  python scripts/enrich_data_summary_synonyms.py --no-llm   # only print skip audit
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
# Same as src/config.py: load API keys before importing src (LLM has no value without .env).
load_dotenv(ROOT / ".env")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_INPUT = ROOT / "config" / "data_summary.json"

_WS = re.compile(r"\s+")


def _phrase_norm(s: str) -> str:
    t = (s or "").replace("\u00a0", " ").strip().lower()
    return _WS.sub(" ", t)


def _strip_json_code_fence(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("```"):
        lines = s.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines)
    return s.strip()


# Paths that are identifiers, blobs, or too vague to ask the LLM for natural slide synonyms.
EXACT_SKIP: frozenset[str] = frozenset(
    {
        "days",
        "customer",
        "generated",
        "github",
        "cohorts_yaml",
        "customer_key_type",
        "salesforce_primary_account_id",
        "salesforce.accounts",
        "salesforce.categories",
        "salesforce.category_errors",
        "teams_yaml.customer_team",
        "teams_yaml.leandna_team",
        "teams_yaml.leandna_site_ids",
    }
)

# Single final path segment considered too abstract when the **entire** path is one segment.
SINGLE_SEGMENT_ABSTRACT_LEAF: frozenset[str] = frozenset(
    {
        "days",
        "customer",
        "type",
        "error",
        "source",
        "github",
        "cohorts_yaml",
    }
)

# Multi-segment paths whose leaf is a technical transport field, not a deck label.
TECHNICAL_SUFFIXES: tuple[str, ...] = (
    ".jql_queries",
    ".base_url",
    ".help_scope",
    ".jsm_organizations_resolved",
)


def should_skip_path(path: str) -> tuple[bool, str]:
    p = (path or "").strip()
    if not p:
        return True, "empty path"
    if p.startswith("_"):
        return True, "internal report key (leading underscore)"
    if p.endswith("[]"):
        return True, "array placeholder path"
    if p in EXACT_SKIP:
        return True, "exact blocklist (id/blob/abstract)"
    for suf in TECHNICAL_SUFFIXES:
        if p.endswith(suf):
            return True, f"technical suffix {suf!r}"
    parts = p.split(".")
    if len(parts) == 1 and parts[0] in SINGLE_SEGMENT_ABSTRACT_LEAF:
        return True, "single-segment abstract leaf"
    return False, ""


def _terms_catalog_notes(terms: list[str]) -> list[str]:
    return [t for t in terms if isinstance(t, str) and t.strip().startswith("[")]


def _terms_non_notes(terms: list[str]) -> list[str]:
    return [t for t in terms if isinstance(t, str) and t.strip() and not t.strip().startswith("[")]


def add_synonyms_to_terms(
    existing_terms: list[str],
    llm_phrases: list[str],
    *,
    max_add: int = 5,
    min_len: int = 4,
) -> tuple[list[str], int]:
    """Append up to ``max_add`` LLM phrases; never remove existing terms."""
    out = [t for t in existing_terms if isinstance(t, str) and t.strip()]
    seen = {_phrase_norm(x) for x in out}
    added = 0
    for ph in llm_phrases:
        if added >= max_add:
            break
        raw = str(ph).strip()
        if len(raw) < min_len:
            continue
        n = _phrase_norm(raw)
        if len(n) < min_len:
            continue
        if n in seen:
            continue
        out.append(raw)
        seen.add(n)
        added += 1
    return out, added


def _call_llm_synonyms(
    paths_with_hints: list[tuple[str, str]],
) -> dict[str, list[str]]:
    """paths_with_hints: (path, short context string from existing terms)."""
    from src.config import LLM_MODEL_FAST, llm_client

    paths = [p for p, _ in paths_with_hints]
    client = llm_client()
    system = (
        "You write SHORT phrases (not numbers) that could appear on a customer QBR slide next to a metric.\n"
        "Audience: **discrete manufacturing** — factories, plants, shop floors, buyers, planners, "
        "material planners, MRP runs, ERP inventory records, work orders, purchase orders, "
        "on-hand / on-order / excess, shortages, late POs, and supply-chain health.\n"
        "Avoid generic consumer-SaaS wording; when the metric is IT/engagement (e.g. users, tickets), "
        "tie language to **plant users, supply-chain collaboration, manufacturing operations** where honest.\n"
        "Each phrase must be at least 4 characters. Output **one JSON object only**, no markdown fences.\n"
        "Schema: {\"suggestions\":[{\"path\":\"<exact input path>\",\"phrases\":[\"...\",\"...\"]}]}\n"
        "Include **every** input path exactly once. For each path return **exactly 5** phrases "
        "(or fewer only if impossible without lying). Do not repeat the dotted path as a phrase. "
        "Do not invent metric definitions that contradict the path hints."
    )
    user = json.dumps(
        {
            "paths": [{"path": p, "catalog_hint": h} for p, h in paths_with_hints],
        },
        indent=2,
    )
    resp = client.chat.completions.create(
        model=LLM_MODEL_FAST,
        temperature=0.25,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    raw = _strip_json_code_fence(resp.choices[0].message.content or "")
    parsed = json.loads(raw)
    out: dict[str, list[str]] = {}
    for row in parsed.get("suggestions") or []:
        if not isinstance(row, dict):
            continue
        p = (row.get("path") or "").strip()
        phs = row.get("phrases") or []
        if isinstance(phs, str):
            phs = [phs]
        out[p.lower()] = [str(x).strip() for x in phs if str(x).strip()]
    # Fail loud if any path missing from response
    missing = [p for p in paths if p.lower() not in out]
    if missing:
        raise ValueError(
            "LLM response missing paths (fail loud): "
            + ", ".join(missing[:12])
            + ("…" if len(missing) > 12 else "")
        )
    return out


def _chunks(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def main() -> int:
    ap = argparse.ArgumentParser(description="LLM synonym enrichment for config/data_summary.json")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to data_summary.json")
    ap.add_argument("--apply", action="store_true", help="Write merged terms back to JSON")
    ap.add_argument("--no-backup", action="store_true", help="Skip .bak when using --apply")
    ap.add_argument("--batch-size", type=int, default=8, help="Paths per LLM request (default 8)")
    ap.add_argument("--max-paths", type=int, default=0, help="Cap paths after skip (0 = all)")
    ap.add_argument("--no-llm", action="store_true", help="Only print skip audit; no API calls")
    args = ap.parse_args()

    path = args.input.resolve()
    if not path.is_file():
        print(f"Input not found: {path}", file=sys.stderr)
        return 1

    data = json.loads(path.read_text(encoding="utf-8"))
    entries = data.get("entries") or []
    if not isinstance(entries, list):
        print("Invalid data_summary.json: entries must be a list", file=sys.stderr)
        return 1

    work: list[dict[str, Any]] = []
    skipped: list[tuple[str, str]] = []
    for ent in entries:
        if not isinstance(ent, dict):
            continue
        p = (ent.get("path") or "").strip()
        if not p:
            continue
        skip, reason = should_skip_path(p)
        if skip:
            skipped.append((p, reason))
            print(f"[skip] {p} — {reason}", file=sys.stderr)
            continue
        work.append(ent)

    if args.max_paths and args.max_paths > 0:
        work = work[: args.max_paths]

    print(f"Catalog entries: {len(entries)} | skipped: {len(skipped)} | to enrich: {len(work)}", file=sys.stderr)

    if args.no_llm:
        print("Dry-run (--no-llm): no LLM calls.", file=sys.stderr)
        return 0

    batch = max(1, min(16, args.batch_size))
    all_llm: dict[str, list[str]] = {}

    try:
        for chunk in _chunks(work, batch):
            hints: list[tuple[str, str]] = []
            for ent in chunk:
                p = str(ent.get("path") or "").strip()
                terms = ent.get("terms") or []
                if not isinstance(terms, list):
                    terms = []
                hint_bits = _terms_non_notes(terms)
                meta = _terms_catalog_notes(terms)
                hint = " ".join(hint_bits[:4])
                if meta:
                    hint = (hint + " " + " ".join(meta[:2])).strip()
                hints.append((p, hint[:1200] or p))
            part = _call_llm_synonyms(hints)
            for k, v in part.items():
                all_llm.setdefault(k, []).extend(v)
    except Exception as e:
        print(f"LLM error: {e}", file=sys.stderr)
        raise

    total_added = 0
    for ent in work:
        p = str(ent.get("path") or "").strip()
        terms = ent.get("terms") or []
        if not isinstance(terms, list):
            terms = []
        seen_before = {_phrase_norm(t) for t in terms if isinstance(t, str)}
        llm_phs = all_llm.get(p.lower(), [])
        merged, nadd = add_synonyms_to_terms(terms, llm_phs, max_add=5)
        total_added += nadd
        print(f"=== {p} ===\n  + {nadd} synonym(s)")
        for t in merged:
            if isinstance(t, str) and _phrase_norm(t) not in seen_before:
                print(f"    → {t}")

    print(f"\nTotal new synonym strings appended: {total_added}", file=sys.stderr)

    if not args.apply:
        print("Dry-run: no file written (use --apply).", file=sys.stderr)
        return 0

    # Merge back into full entries list (preserve order)
    by_path_lower = {str(e.get("path") or "").strip().lower(): e for e in work}
    new_entries: list[dict[str, Any]] = []
    for ent in entries:
        if not isinstance(ent, dict):
            continue
        p = (ent.get("path") or "").strip()
        key = p.lower()
        if key in by_path_lower:
            src = by_path_lower[key]
            terms = src.get("terms") or []
            llm_phs = all_llm.get(p.lower(), [])
            merged, _ = add_synonyms_to_terms(
                terms if isinstance(terms, list) else [],
                llm_phrases=llm_phs,
                max_add=5,
            )
            new_entries.append({"path": p, "terms": merged})
        else:
            new_entries.append(dict(ent))

    data["entries"] = new_entries
    try:
        v = int(data.get("version", 0))
    except (TypeError, ValueError):
        v = 0
    data["version"] = v + 1
    note = (
        "Catalog paths with ``terms``; synonym phrases may be LLM-suggested "
        "(manufacturing / MRP-ERP context). Regenerate skeleton via "
        "scripts/generate_data_summary_catalog.py; enrich via scripts/enrich_data_summary_synonyms.py."
    )
    data["_comment"] = note

    if not args.no_backup:
        bak = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(path, bak)
        print(f"Backup: {bak}", file=sys.stderr)

    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {path} version={data['version']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
