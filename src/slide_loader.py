"""Load slide definitions from YAML files.

Sources (in priority order):
  1. Google Drive  bpo-config/slides/  (user-editable; repo wins on first load each run)
  2. Local repo    slides/             (canonical defaults)

If a Drive file fails to parse, the local version is used and a QA warning
is raised so the discrepancy shows up on the Data Quality slide.
"""

from pathlib import Path
from typing import Any

import yaml

from .config import GOOGLE_DRIVE_FOLDER_ID, logger

DEFAULT_SLIDES_DIR = Path(__file__).resolve().parent.parent / "slides"

_USE_DRIVE = bool(GOOGLE_DRIVE_FOLDER_ID)


def _parse_order(order_val: Any) -> tuple[int, str]:
    """Normalize the order field into a sortable (priority, ref) tuple.

    Numeric values sort directly. 'after <id>' resolves during ordering.
    """
    if isinstance(order_val, (int, float)):
        return (int(order_val), "")
    s = str(order_val).strip()
    if s.isdigit():
        return (int(s), "")
    if s.lower().startswith("after "):
        return (9000, s[6:].strip())
    return (5000, "")


def _load_all_slides(slides_dir: str | Path | None = None) -> list[dict[str, Any]]:
    """Load slide definitions from Drive (with local fallback) or purely local."""
    d = Path(slides_dir) if slides_dir else DEFAULT_SLIDES_DIR
    if _USE_DRIVE and not slides_dir:
        try:
            from .drive_config import load_yaml_from_drive
            return load_yaml_from_drive("slides", d)
        except Exception as e:
            logger.warning("Drive slide load failed, falling back to local: %s", e)

    if not d.is_dir():
        logger.warning("Slides directory not found: %s", d)
        return []

    results: list[dict[str, Any]] = []
    for f in sorted(d.glob("*.yaml")):
        try:
            raw = yaml.safe_load(f.read_text())
            if isinstance(raw, dict) and "id" in raw:
                raw["_file"] = f.name
                raw["_source"] = "local"
                results.append(raw)
        except Exception as e:
            logger.warning("Skipping malformed slide %s: %s", f.name, e)
    return results


def load_slides(
    slides_dir: str | Path | None = None,
    customer: str | None = None,
) -> list[dict[str, Any]]:
    """Load all active slide definitions, optionally filtered for a specific customer.

    Args:
        slides_dir: Path to the slides folder. Defaults to project root /slides.
        customer: If provided, only return slides that apply to this customer.

    Returns:
        Sorted list of slide dicts.
    """
    slides = _load_all_slides(slides_dir)

    if customer:
        slides = _filter_for_customer(slides, customer)

    return _sort_slides(slides)


def _filter_for_customer(
    slides: list[dict], customer: str
) -> list[dict]:
    """Keep only slides whose 'customers' field matches this customer."""
    result = []
    for r in slides:
        target = r.get("customers", "all")
        if target == "all":
            result.append(r)
        elif isinstance(target, list):
            if any(c.lower() == customer.lower() for c in target):
                result.append(r)
        elif isinstance(target, str) and target.lower() == customer.lower():
            result.append(r)
    return result


def _sort_slides(slides: list[dict]) -> list[dict]:
    """Sort slides: numeric order first, then 'after X' relative to their anchor."""
    parsed = [(r, _parse_order(r.get("order", 5000))) for r in slides]

    absolute = [(r, pri) for r, (pri, ref) in parsed if not ref]
    relative = [(r, ref) for r, (_, ref) in parsed if ref]

    absolute.sort(key=lambda x: x[1])
    result = [r for r, _ in absolute]

    id_to_idx = {r["id"]: i for i, r in enumerate(result)}
    for r, ref in relative:
        anchor_idx = id_to_idx.get(ref)
        if anchor_idx is not None:
            insert_at = anchor_idx + 1
        else:
            insert_at = len(result)
        result.insert(insert_at, r)
        id_to_idx = {r["id"]: i for i, r in enumerate(result)}

    return result


def get_slide_prompts(
    customer: str,
    slides_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return a simplified list of slide definitions for the agent to consume.

    Each entry has: id, type, title, slide_type, data_tools, prompt.
    """
    slides = load_slides(slides_dir=slides_dir, customer=customer)
    return [
        {
            "id": r["id"],
            "type": r.get("type", "standard"),
            "title": r.get("title", r["id"]),
            "slide_type": r.get("slide_type", r["id"]),
            "data_tools": r.get("data_tools", []),
            "prompt": r.get("prompt", "").strip(),
        }
        for r in slides
    ]
