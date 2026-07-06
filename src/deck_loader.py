"""Load deck definitions from YAML files.

Sources (in priority order):
  1. Google Drive  ``<QBR Generator>/decks/`` (see ``get_qbr_generator_folder_id_for_drive_config`` in drive_config; repo wins on first load each run)
  2. Local repo    decks/             (canonical defaults)

If a Drive file fails to parse, the local version is used and a QA warning
is raised so the discrepancy shows up on the Data Quality slide.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID, logger
from .slide_loader import load_slides

DEFAULT_DECKS_DIR = Path(__file__).resolve().parent.parent / "decks"

_USE_DRIVE = bool(GOOGLE_QBR_GENERATOR_FOLDER_ID)


def _load_all_decks(decks_dir: str | Path | None = None) -> list[dict[str, Any]]:
    """Load deck definitions from Drive (with local fallback) or purely local."""
    d = Path(decks_dir) if decks_dir else DEFAULT_DECKS_DIR
    if _USE_DRIVE and not decks_dir:
        try:
            from .drive_config import load_yaml_from_drive
            return load_yaml_from_drive("decks", d)
        except Exception as e:
            logger.warning("Drive deck load failed, falling back to local: %s", e)

    if not d.is_dir():
        logger.warning("Decks directory not found: %s", d)
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
            logger.warning("Skipping malformed deck %s: %s", f.name, e)
    return results


def list_decks(
    decks_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return a summary of all available deck definitions.

    Each entry has: id, name, audience, purpose (first 200 chars).
    """
    return [
        {
            "id": raw["id"],
            "name": raw.get("name", raw["id"]),
            "audience": raw.get("audience", ""),
            "purpose": raw.get("purpose", "").strip()[:200],
            "_file": raw.get("_file", ""),
            "_source": raw.get("_source", "local"),
        }
        for raw in _load_all_decks(decks_dir)
    ]


def _slide_ids_required_by_deck(deck: dict[str, Any]) -> set[str]:
    """Collect slide ``recipe``/``slide`` ids from a deck that need a slide YAML (not excluded)."""
    overrides: dict[str, Any] = {}
    for o in deck.get("overrides", []) or []:
        rid = o.get("slide", o.get("recipe", ""))
        if rid:
            overrides[str(rid)] = o
    out: set[str] = set()
    for entry in deck.get("slides", []) or []:
        rid = entry.get("slide", entry.get("recipe", ""))
        if not rid:
            continue
        if overrides.get(rid, {}).get("exclude"):
            continue
        out.add(str(rid))
    return out


def _merge_extended_deck(alias: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    """Merge an alias deck (``extends: <base_id>``) onto its base definition."""
    merged = dict(base)
    for key in ("id", "name", "audience", "purpose", "_file", "_source"):
        if key in alias and alias.get(key) is not None:
            merged[key] = alias[key]
    if alias.get("slides"):
        merged["slides"] = [dict(e) for e in alias["slides"]]
    else:
        merged["slides"] = [dict(e) for e in (base.get("slides") or [])]

    cover_title = alias.get("cover_title")
    if cover_title:
        for i, entry in enumerate(merged["slides"]):
            rid = entry.get("slide", entry.get("recipe", ""))
            if rid == "support_deck_cover":
                merged["slides"][i] = {**entry, "title": str(cover_title).strip()}
                break

    slide_title_overrides = alias.get("slide_title_overrides") or {}
    if slide_title_overrides:
        for i, entry in enumerate(merged["slides"]):
            rid = entry.get("slide", entry.get("recipe", ""))
            ot = slide_title_overrides.get(rid)
            if ot:
                merged["slides"][i] = {**entry, "title": str(ot).strip()}

    for key in ("extends", "cover_title", "slide_title_overrides"):
        merged.pop(key, None)
    return merged


def _apply_deck_extends(
    deck: dict[str, Any],
    decks_dir: str | Path | None,
    *,
    _stack: frozenset[str] | None = None,
) -> dict[str, Any]:
    """When deck YAML declares ``extends: <id>``, inherit slides from the base deck."""
    extends = deck.get("extends")
    if not extends:
        return deck
    base_id = str(extends).strip()
    if not base_id:
        return deck
    deck_id = str(deck.get("id") or "")
    seen = _stack or frozenset()
    if deck_id in seen or base_id in seen:
        logger.warning("Deck '%s' has circular extends chain; using alias YAML as-is", deck_id)
        return deck
    base = load_deck(base_id, decks_dir, _extends_stack=seen | {deck_id})
    if not base:
        logger.warning("Deck '%s' extends missing deck '%s'", deck_id, base_id)
        return deck
    return _merge_extended_deck(deck, base)


def _load_deck_from_local_dir(d: Path, deck_id: str) -> dict[str, Any] | None:
    """Load one deck from the repo ``decks/`` tree without a Drive folder walk."""
    if not d.is_dir():
        return None
    direct = d / f"{deck_id}.yaml"
    if direct.is_file():
        try:
            raw = yaml.safe_load(direct.read_text())
            if isinstance(raw, dict) and raw.get("id") == deck_id:
                raw.setdefault("_file", direct.name)
                raw.setdefault("_source", "local")
                return raw
        except Exception as e:
            logger.debug("load_deck: %s: %s", direct, e)
    for f in sorted(d.glob("*.yaml")):
        if f.name == direct.name:
            continue
        try:
            raw = yaml.safe_load(f.read_text())
            if isinstance(raw, dict) and raw.get("id") == deck_id:
                raw.setdefault("_file", f.name)
                raw.setdefault("_source", "local")
                return raw
        except Exception as e:
            logger.debug("load_deck: %s: %s", f.name, e)
    return None


def load_deck(
    deck_id: str,
    decks_dir: str | Path | None = None,
    *,
    _extends_stack: frozenset[str] | None = None,
) -> dict[str, Any] | None:
    """Load a single deck definition by ID (one YAML file — never the full Drive decks folder)."""
    d = Path(decks_dir) if decks_dir else DEFAULT_DECKS_DIR
    raw: dict[str, Any] | None = None
    local = _load_deck_from_local_dir(d, deck_id)
    if local:
        raw = local
    elif _USE_DRIVE and not decks_dir:
        try:
            from .drive_config import load_deck_yaml_from_drive

            got = load_deck_yaml_from_drive(deck_id, d)
            if got and got.get("id") == deck_id:
                raw = got
        except Exception as e:
            logger.debug("load_deck: Drive single fetch: %s", e)
    if not raw:
        return None
    return _apply_deck_extends(raw, decks_dir, _stack=_extends_stack)


def resolve_deck(
    deck_id: str,
    customer: str,
    decks_dir: str | Path | None = None,
    slides_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Resolve a deck definition into a concrete slide plan for a customer.

    Loads the deck, loads all applicable slide definitions for the customer,
    then applies the deck's slide list and override rules to produce
    a final ordered list of slide prompts the agent should follow.

    Returns:
        {
            id, name, audience, purpose,
            slides: [{id, type, title, slide_type, data_tools, prompt, required, note}],
            excluded: [{slide, note}],
        }
    """
    deck = load_deck(deck_id, decks_dir)
    if not deck:
        return {"error": f"Deck '{deck_id}' not found"}

    need_ids = _slide_ids_required_by_deck(deck)
    all_slides = load_slides(
        slides_dir=slides_dir,
        customer=customer,
        only_slide_ids=need_ids,
    )
    slide_map = {r["id"]: r for r in all_slides}

    overrides = {}
    for o in deck.get("overrides", []):
        rid = o.get("slide", o.get("recipe", ""))
        if rid:
            overrides[rid] = o

    slides = []
    excluded = []
    seen_in_slides = set()

    id_counts: dict[str, int] = {}

    for entry in deck.get("slides", []):
        rid = entry.get("slide", entry.get("recipe", ""))
        seen_in_slides.add(rid)
        override = overrides.get(rid, {})

        if override.get("exclude"):
            excluded.append({"slide": rid, "note": override.get("note", "")})
            continue

        slide_def = slide_map.get(rid)
        if not slide_def:
            logger.debug("Slide '%s' referenced in deck but not found for customer '%s'", rid, customer)
            continue

        # Generate unique ID when a slide type appears multiple times
        id_counts[rid] = id_counts.get(rid, 0) + 1
        unique_id = f"{rid}_{id_counts[rid]}" if id_counts[rid] > 1 else rid

        # Deck entry fields (e.g. title) override slide definition defaults
        resolved_title = entry.get("title", slide_def.get("title", slide_def["id"]))

        slide_row = {
            "id": unique_id,
            "type": slide_def.get("type", "standard"),
            "title": resolved_title,
            "slide_type": slide_def.get("slide_type", slide_def["id"]),
            "data_tools": slide_def.get("data_tools", []),
            "prompt": slide_def.get("prompt", "").strip(),
            "required": override.get("require", False),
            "note": entry.get("note", override.get("note", "")),
        }
        # Deck YAML may pass builder params (e.g. jira_project for eng_jira_project slides).
        if "jira_project" in entry:
            slide_row["jira_project"] = str(entry["jira_project"]).strip().upper()
        ct = entry.get("cover_title")
        if ct:
            slide_row["cover_title"] = str(ct).strip()
        if slide_def.get("sf_category"):
            slide_row["sf_category"] = str(slide_def["sf_category"]).strip()
        if entry.get("sf_category"):
            slide_row["sf_category"] = str(entry["sf_category"]).strip()
        if slide_def.get("intro_blurb") is not None:
            slide_row["intro_blurb"] = str(slide_def.get("intro_blurb") or "").strip()
        if slide_def.get("notable_subtitle") is not None:
            slide_row["notable_subtitle"] = str(slide_def.get("notable_subtitle") or "").strip()
        ni = slide_def.get("notable_items")
        if ni:
            slide_row["notable_items"] = [str(x).strip() for x in ni if str(x).strip()][:6]
        slides.append(slide_row)

    for rid, override in overrides.items():
        if override.get("exclude") and rid not in seen_in_slides:
            excluded.append({"slide": rid, "note": override.get("note", "")})

    return {
        "id": deck["id"],
        "name": deck.get("name", deck["id"]),
        "audience": deck.get("audience", ""),
        "purpose": deck.get("purpose", "").strip(),
        "slides": slides,
        "excluded": excluded,
    }
