#!/usr/bin/env python3
"""LLM-assisted synonym phrases for ``config/data_field_synonyms.json``.

Default: print existing vs LLM suggestions (dry-run, no writes).

With ``--apply``: merge suggestions into the config with duplicate-safe rules, bump ``version``,
and write JSON (backup ``.bak`` unless ``--no-backup``).

Usage (from repo root):
  python scripts/suggest_data_field_synonyms.py
  python scripts/suggest_data_field_synonyms.py --max-paths 10
  python scripts/suggest_data_field_synonyms.py --apply --all-entry-paths
  python scripts/suggest_data_field_synonyms.py --apply --all-entry-paths --batch-size 10
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_WS = re.compile(r"\s+")


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


def _phrase_norm(s: str) -> str:
    """Lowercase + collapsed whitespace for duplicate detection."""
    t = (s or "").replace("\u00a0", " ").strip().lower()
    return _WS.sub(" ", t)


def _load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _existing_phrases_by_path(entries: list) -> dict[str, list[str]]:
    by_path: dict[str, list[str]] = defaultdict(list)
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        p = (e.get("path") or "").strip()
        if not p:
            continue
        key = p.lower()
        for ph in e.get("phrases") or []:
            if isinstance(ph, str) and ph.strip():
                by_path[key].append(ph.strip())
    return dict(by_path)


def _scalar_reference_paths(ref: list) -> list[str]:
    skip = frozenset({"json_array", "json_object"})
    out: list[str] = []
    for row in ref or []:
        if not isinstance(row, dict):
            continue
        t = (row.get("type") or "").strip().lower()
        if t in skip:
            continue
        p = (row.get("path") or "").strip()
        if p:
            out.append(p)
    return out


def _all_entry_paths_in_order(entries: list) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        p = (e.get("path") or "").strip()
        if not p:
            continue
        k = p.lower()
        if k not in seen:
            seen.add(k)
            out.append(p)
    return out


def _pick_paths(data: dict, max_paths: int) -> list[str]:
    entries = data.get("entries") or []
    ref = data.get("data_summary_paths_reference") or []
    ordered: list[str] = []
    seen: set[str] = set()
    for e in entries:
        if not isinstance(e, dict):
            continue
        p = (e.get("path") or "").strip()
        if not p:
            continue
        k = p.lower()
        if k not in seen:
            seen.add(k)
            ordered.append(p)
        if len(ordered) >= max_paths:
            return ordered
    for p in _scalar_reference_paths(ref):
        k = p.lower()
        if k not in seen:
            seen.add(k)
            ordered.append(p)
        if len(ordered) >= max_paths:
            break
    return ordered[:max_paths]


def _reference_rows_for_paths(ref: list, paths: list[str]) -> list[dict]:
    want = {p.lower() for p in paths}
    out = []
    for row in ref or []:
        if not isinstance(row, dict):
            continue
        p = (row.get("path") or "").strip()
        if p.lower() in want:
            out.append({"path": p, "type": row.get("type", ""), "note": row.get("note", "")})
    return out


def merge_phrases_intelligent(
    existing: list[str],
    incoming: list[str],
) -> tuple[list[str], dict[str, int]]:
    """Merge incoming into existing without dropping curated ``existing`` rows.

    Rules (normalized strings):
    - Exact duplicate (incoming vs current list) → skip incoming.
    - If incoming is a strict substring of an existing phrase → skip incoming (keep existing wording).
    - **Never** remove an existing phrase because a longer LLM phrase contains it (preserves hand-tuned
      short labels like ``cost avoidance`` next to ``material cost avoidance``).

    Within **incoming only**, later phrases that duplicate or are subsumed by an earlier incoming
    phrase are skipped to avoid LLM self-duplication.
    """
    result = [x.strip() for x in existing if isinstance(x, str) and x.strip()]
    stats = {"added": 0, "skipped_duplicate": 0, "skipped_subsumed_by_existing": 0, "skipped_incoming_redundant": 0}

    incoming_kept: list[str] = []
    incoming_norms: list[str] = []
    for p in incoming:
        p = str(p).strip()
        np = _phrase_norm(p)
        if len(np) < 4:
            continue
        dup = False
        for ik in incoming_norms:
            if np == ik:
                dup = True
                break
            if len(np) < len(ik) and np in ik:
                dup = True
                break
            if len(ik) < len(np) and ik in np:
                dup = True
                break
        if dup:
            stats["skipped_incoming_redundant"] += 1
            continue
        incoming_kept.append(p)
        incoming_norms.append(np)

    for p in incoming_kept:
        np = _phrase_norm(p)
        if any(_phrase_norm(x) == np for x in result):
            stats["skipped_duplicate"] += 1
            continue

        subsumed_by_existing = False
        for e in result:
            en = _phrase_norm(e)
            if np in en and len(np) < len(en):
                subsumed_by_existing = True
                break
        if subsumed_by_existing:
            stats["skipped_subsumed_by_existing"] += 1
            continue

        result.append(p)
        stats["added"] += 1

    return result, stats


def _call_llm(paths: list[str], ref_subset: list[dict], existing: dict[str, list[str]]) -> dict:
    from src.config import LLM_MODEL_FAST, llm_client

    client = llm_client()
    existing_compact = {p: existing.get(p.lower(), []) for p in paths}
    system = (
        "You propose SHORT slide phrases (not numeric values) that could appear next to a metric on a "
        "customer QBR deck for a **manufacturing / industrial** account using LeanDNA-style supply-chain "
        "and inventory tooling.\n"
        "Strongly prefer wording rooted in: **factories, plants, manufacturing sites, supply chain, "
        "inventory, materials, buyers/planners, production, MRP-adjacent language, on-hand / on-order, "
        "shortages, excess, POs, and operational health** — not generic consumer-SaaS or marketing fluff.\n"
        "When a path is clearly about engagement or IT (e.g. visitors, tickets), keep accurate meaning but "
        "you may tie to **plant users, buyers on the floor, supply-chain collaboration** where it fits.\n"
        "Each phrase must be >= 4 characters. Output ONE JSON object only, no markdown fences. "
        "Do not invent new data paths. For each path, suggest 5–10 NEW phrases that do NOT duplicate "
        "the existing list (paraphrases OK if clearly distinct). "
        'Schema: {"suggestions":[{"path":"string","phrases":["..."]}]} — include every input path exactly once.'
    )
    user = json.dumps(
        {
            "context": (
                "Audience: manufacturing customer success QBR; metrics often tie to factories and inventory."
            ),
            "paths_to_expand": paths,
            "path_definitions": ref_subset,
            "existing_phrases_by_path": existing_compact,
        },
        indent=2,
    )
    resp = client.chat.completions.create(
        model=LLM_MODEL_FAST,
        temperature=0.3,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    raw = _strip_json_code_fence(resp.choices[0].message.content or "")
    return json.loads(raw)


def _parse_suggestions(parsed: dict) -> dict[str, list[str]]:
    by_llm: dict[str, list[str]] = {}
    for row in parsed.get("suggestions") or []:
        if not isinstance(row, dict):
            continue
        p = (row.get("path") or "").strip()
        phs = row.get("phrases") or []
        if isinstance(phs, str):
            phs = [phs]
        by_llm[p.lower()] = [str(x).strip() for x in phs if str(x).strip()]
    return by_llm


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Suggest or merge data_field_synonyms phrases via LLM.",
    )
    ap.add_argument(
        "--config",
        type=Path,
        default=ROOT / "config" / "data_field_synonyms.json",
        help="Path to data_field_synonyms.json",
    )
    ap.add_argument(
        "--max-paths",
        type=int,
        default=10,
        help="Max paths per dry-run, or when not using --all-entry-paths (default 10)",
    )
    ap.add_argument(
        "--all-entry-paths",
        action="store_true",
        help="Use every path that appears in entries (chunked for LLM)",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Paths per LLM request when using --all-entry-paths (default 10)",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Merge suggestions into config, bump version, write file",
    )
    ap.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not write .bak before --apply",
    )
    ap.add_argument(
        "--no-llm",
        action="store_true",
        help="Print existing phrases only (no API call)",
    )
    args = ap.parse_args()

    if args.apply and args.no_llm:
        print("Cannot use --apply with --no-llm.", file=sys.stderr)
        return 2

    path = args.config.resolve()
    if not path.is_file():
        print(f"Config not found: {path}", file=sys.stderr)
        return 1

    data = _load_config(path)
    ref = data.get("data_summary_paths_reference") or []
    entries = data.get("entries") or []

    if args.all_entry_paths:
        paths = _all_entry_paths_in_order(entries)
    else:
        paths = _pick_paths(data, max(1, min(100, args.max_paths)))

    existing = _existing_phrases_by_path(entries)

    print(f"Config: {path}")
    print(f"Paths ({len(paths)}): {', '.join(paths[:15])}{'…' if len(paths) > 15 else ''}")
    print()

    if args.no_llm:
        for p in paths:
            ex = existing.get(p.lower(), [])
            print(f"=== {p} ===")
            print("Existing:")
            for ph in ex:
                print(f"  - {ph}")
            print("LLM suggested: (skipped --no-llm)")
            print()
        return 0

    batch = max(1, min(25, args.batch_size))
    all_llm: dict[str, list[str]] = {}
    try:
        for chunk in _chunks(paths, batch):
            ref_subset = _reference_rows_for_paths(ref, chunk)
            parsed = _call_llm(chunk, ref_subset, existing)
            part = _parse_suggestions(parsed)
            for k, v in part.items():
                all_llm.setdefault(k, []).extend(v)
    except Exception as e:
        print(f"LLM call failed: {e}", file=sys.stderr)
        return 1

    def _norm_set(phs: list[str]) -> set[str]:
        return {_phrase_norm(x) for x in phs}

    for p in paths:
        ex = existing.get(p.lower(), [])
        llm_phs = all_llm.get(p.lower(), [])
        print(f"=== {p} ===")
        print("Existing:")
        if ex:
            for ph in ex:
                print(f"  - {ph}")
        else:
            print("  (none in config)")
        print("LLM suggested (raw):")
        if llm_phs:
            for ph in llm_phs:
                print(f"  - {ph}")
        else:
            print("  (no suggestions returned for this path)")

        merged, st = merge_phrases_intelligent(ex, llm_phs)
        ex_n = _norm_set(ex)
        merged_n = _norm_set(merged)
        added_norms = merged_n - ex_n
        print("After merge (dedupe + substring rules):")
        for ph in merged:
            if _phrase_norm(ph) in added_norms:
                print(f"  + {ph}")
        removed = [x for x in ex if x not in merged]
        for ph in sorted(removed, key=lambda s: len(_phrase_norm(s))):
            print(f"  − removed (subsumed or replaced): {ph}")
        if not added_norms and not removed:
            print("  (no net change)")
        print()

    if not args.apply:
        print("Dry-run: no files modified (use --apply to write).")
        return 0

    # Apply merges into entries (single merge pass; stats from this pass only)
    new_entries: list[dict] = []
    total_stats = defaultdict(int)
    for e in entries:
        if not isinstance(e, dict):
            continue
        p = (e.get("path") or "").strip()
        if not p:
            continue
        phrases = [x for x in (e.get("phrases") or []) if isinstance(x, str) and x.strip()]
        llm_phs = all_llm.get(p.lower(), [])
        merged, st = merge_phrases_intelligent(phrases, llm_phs)
        for k, v in st.items():
            total_stats[k] += v
        new_entries.append({"path": p, "phrases": merged})

    data["entries"] = new_entries
    try:
        v = int(data.get("version", 0))
    except (TypeError, ValueError):
        v = 0
    data["version"] = v + 1

    if not args.no_backup:
        bak = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(path, bak)
        print(f"Backup: {bak}")

    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {path} (version={data['version']})")
    print(
        "Merge stats: "
        + ", ".join(f"{k}={total_stats[k]}" for k in sorted(total_stats) if total_stats[k])
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
