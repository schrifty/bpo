#!/usr/bin/env python3
"""Inspect the slide analysis cache (.slide_cache/).

Usage:
  python scripts/inspect_slide_cache.py              # summary of all entries
  python scripts/inspect_slide_cache.py --full      # full JSON for each entry
  python scripts/inspect_slide_cache.py --classification  # only classification cache
  python scripts/inspect_slide_cache.py --adapt      # only adapt cache
  python scripts/inspect_slide_cache.py --analysis   # only broad analysis (data ask + purpose)
  python scripts/inspect_slide_cache.py --hash abc  # only entries whose hash starts with 'abc'
"""
import argparse
import json
import sys
from pathlib import Path

# Project root (script lives in scripts/)
ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / ".slide_cache"


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _classification_summary(data: dict) -> str:
    st = data.get("slide_type", "?")
    title = (data.get("title") or "")[:50]
    return f"  {st}: {title!r}"


def _adapt_summary(data: dict) -> str:
    reps = data.get("replacements", [])
    n = len(reps)
    mapped = sum(1 for r in reps if r.get("mapped"))
    return f"  {n} replacements ({mapped} mapped)"


def _analysis_summary(data: dict) -> str:
    purpose = (data.get("purpose") or "")[:60]
    st = data.get("slide_type", "?")
    ask = data.get("data_ask", [])
    keys = [a.get("key", "?") for a in ask[:8]]
    return f"  {st}: {len(ask)} data items — {purpose!r}\n  keys: {keys}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect .slide_cache (classification and adapt entries)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Print full JSON for each entry",
    )
    parser.add_argument(
        "--classification",
        action="store_true",
        help="Only show classification cache",
    )
    parser.add_argument(
        "--adapt",
        action="store_true",
        help="Only show adapt cache",
    )
    parser.add_argument(
        "--analysis",
        action="store_true",
        help="Only show broad slide analysis cache (data ask + purpose)",
    )
    parser.add_argument(
        "--hash",
        metavar="PREFIX",
        default="",
        help="Only show entries whose hash starts with PREFIX",
    )
    args = parser.parse_args()

    if not CACHE_DIR.exists():
        print(f"Cache directory not found: {CACHE_DIR}")
        print("Run a hydrate first to populate the cache.")
        return 0

    show_analysis = args.analysis
    show_class = args.classification
    show_adapt = args.adapt
    if not (show_analysis or show_class or show_adapt):
        show_class = show_adapt = show_analysis = True
    prefix = (args.hash or "").lower()

    total = 0

    if show_class:
        class_dir = CACHE_DIR / "classification"
        if class_dir.exists():
            files = sorted(class_dir.glob("*.json"))
            files = [f for f in files if f.stem.startswith(prefix)] if prefix else files
            print(f"Classification cache ({len(files)} entries)")
            print("-" * 50)
            for f in files:
                total += 1
                data = _read_json(f)
                if data is None:
                    print(f"  {f.stem}: (invalid JSON)")
                    continue
                if args.full:
                    print(f"--- {f.stem} ---")
                    print(json.dumps(data, indent=2, default=str))
                    print()
                else:
                    print(f"  {f.stem[:16]}...")
                    print(_classification_summary(data))
            if files and not args.full:
                print()
        else:
            print("Classification cache: (empty)")

    if show_analysis:
        analysis_dir = CACHE_DIR / "analysis"
        if analysis_dir.exists():
            files = sorted(analysis_dir.glob("*.json"))
            files = [f for f in files if f.stem.startswith(prefix)] if prefix else files
            print(f"Analysis cache ({len(files)} entries)")
            print("-" * 50)
            for f in files:
                total += 1
                data = _read_json(f)
                if data is None:
                    print(f"  {f.stem}: (invalid JSON)")
                    continue
                if "_version" in data:
                    data = {k: v for k, v in data.items() if not k.startswith("_")}
                if args.full:
                    print(f"--- {f.stem} ---")
                    print(json.dumps(data, indent=2, default=str))
                    print()
                else:
                    print(f"  {f.stem[:16]}...")
                    print(_analysis_summary(data))
            if files and not args.full:
                print()
        else:
            print("Analysis cache: (empty)")

    if show_adapt:
        adapt_dir = CACHE_DIR / "adapt"
        if adapt_dir.exists():
            files = sorted(adapt_dir.glob("*.json"))
            files = [f for f in files if f.stem.startswith(prefix)] if prefix else files
            print(f"Adapt cache ({len(files)} entries)")
            print("-" * 50)
            for f in files:
                total += 1
                data = _read_json(f)
                if data is None:
                    print(f"  {f.stem}: (invalid JSON)")
                    continue
                if args.full:
                    print(f"--- {f.stem} ---")
                    print(json.dumps(data, indent=2, default=str))
                    print()
                else:
                    print(f"  {f.stem[:16]}...")
                    print(_adapt_summary(data))
            if files and not args.full:
                print()
        else:
            print("Adapt cache: (empty)")

    if prefix and total == 0:
        print(f"No entries with hash prefix {args.hash!r}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
