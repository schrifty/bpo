#!/usr/bin/env python3
"""Export slide thumbnails for a given presentation.

Usage:
    python scripts/export_thumbnails.py <presentation-id-or-url> [output_dir]
"""
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(project_root) / ".env")


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        return 1

    pres_id = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    from src.slides_client import export_slide_thumbnails
    saved = export_slide_thumbnails(pres_id, output_dir=output_dir)
    for p in saved:
        print(f"  {p}")
    print(f"\n{len(saved)} slides exported.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
