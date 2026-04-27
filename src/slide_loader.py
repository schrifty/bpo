"""Load slide definitions from YAML files.

Sources (in priority order):
  1. Google Drive  ``<QBR Generator>/slides/`` (see ``get_qbr_generator_folder_id_for_drive_config`` in drive_config; repo wins on first load each run)
  2. Local repo    slides/             (canonical defaults)

If a Drive file fails to parse, the local version is used and a QA warning
is raised so the discrepancy shows up on the Data Quality slide.
"""

from __future__ import annotations

import copy
import functools
from pathlib import Path
from typing import Any

import yaml

from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID, logger

DEFAULT_SLIDES_DIR = Path(__file__).resolve().parent.parent / "slides"

_USE_DRIVE = bool(GOOGLE_QBR_GENERATOR_FOLDER_ID)


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


def _load_all_slides(
    slides_dir: str | Path | None = None,
    *,
    only_slide_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Load slide definitions from Drive (with local fallback) or purely local.

    If ``only_slide_ids`` is set, only definitions whose ``id`` is in that set are
    read (faster for a single known deck; see :func:`resolve_deck`).
    """
    d = Path(slides_dir) if slides_dir else DEFAULT_SLIDES_DIR
    if only_slide_ids is not None and not only_slide_ids:
        return []
    if _USE_DRIVE and not slides_dir:
        try:
            from .drive_config import load_yaml_from_drive
            return load_yaml_from_drive(
                "slides",
                d,
                only_slide_ids=only_slide_ids,
            )
        except Exception as e:
            logger.warning("Drive slide load failed, falling back to local: %s", e)

    if not d.is_dir():
        logger.warning("Slides directory not found: %s", d)
        return []

    if only_slide_ids:
        from .drive_config import load_local_slide_definitions_for_ids
        return load_local_slide_definitions_for_ids(d, only_slide_ids)

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
    *,
    only_slide_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Load all active slide definitions, optionally filtered for a specific customer.

    Args:
        slides_dir: Path to the slides folder. Defaults to project root /slides.
        customer: If provided, only return slides that apply to this customer.
                 If None, returns all slides with customers="all".
        only_slide_ids: If set, only load these slide ids (from deck YAML). Skips
            reading the rest of ``slides/`` on Drive or disk — use with :func:`resolve_deck`.

    Returns:
        Sorted list of slide dicts.
    """
    slides = _load_all_slides(slides_dir, only_slide_ids=only_slide_ids)

    if customer is not None:
        slides = _filter_for_customer(slides, customer)
    else:
        # When customer is None, only include slides with customers="all"
        slides = [s for s in slides if s.get("customers") == "all"]

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


def _merge_int_rollup_params(slide_id: str, defaults: dict[str, int]) -> dict[str, int]:
    """Merge ``rollup_params`` from the slide YAML into ``defaults`` (unknown keys ignored)."""
    out = dict(defaults)
    slides = _load_all_slides()
    for r in slides:
        if r.get("id") != slide_id:
            continue
        rp = r.get("rollup_params")
        if not isinstance(rp, dict):
            break
        for key in defaults:
            if key not in rp:
                continue
            v = rp[key]
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)):
                iv = int(v)
                if iv >= 0:
                    out[key] = iv
        break
    return out


_COHORT_FINDINGS_ROLLUP_DEFAULTS: dict[str, int] = {
    "min_customers_for_cross_cohort_compare": 5,
    "min_login_spread_pp": 5,
    "singleton_n": 1,
    "thin_sample_n": 2,
}

# Defaults mirror ``slides/cohort-02-findings.yaml`` ``metadata:`` block.
_COHORT_FINDINGS_METADATA_DEFAULTS: dict[str, Any] = {
    "max_bullets": 1,
    "priority": [
        "single_bucket",
        "singleton",
        "thin_sample",
        "unclassified",
        "provenance",
    ],
    "singleton_list_max": 8,
    "thin_list_max": 6,
    "templates": {
        "single_bucket": (
            "Only one cohort bucket has customers in this window — compare across cohorts when more accounts load."
        ),
        "singleton_one": (
            "Singleton cohorts (one account in-window): {names}{ellipsis} — treat as directional only."
        ),
        "singleton_many": (
            "Cohort buckets with exactly {singleton_n} customers in-window: {names}{ellipsis} "
            "— treat as directional only."
        ),
        "thin_sample": (
            "Thin samples (exactly {thin_n} customers): {names}{ellipsis} — medians are fragile."
        ),
        "unclassified": (
            "{n} customer(s) are unclassified — add or alias them in cohorts.yaml to benchmark by industry cohort."
        ),
        "provenance": (
            "Cohort labels and membership come from cohorts.yaml and docs/CUSTOMER_COHORTS.md — "
            "not redefined in this deck."
        ),
    },
}


@functools.lru_cache(maxsize=1)
def cohort_findings_rollup_params() -> dict[str, int]:
    """Rollup tuning for :func:`compute_cohort_portfolio_rollup` (cohort findings slide).

    Source: ``rollup_params`` on the ``cohort_findings`` slide YAML
    (``slides/cohort-02-findings.yaml`` or Drive). See ``_COHORT_FINDINGS_ROLLUP_DEFAULTS``.
    """
    return _merge_int_rollup_params("cohort_findings", _COHORT_FINDINGS_ROLLUP_DEFAULTS)


@functools.lru_cache(maxsize=1)
def cohort_findings_metadata() -> dict[str, Any]:
    """Cohort metadata bullets for :func:`compute_cohort_portfolio_rollup` (templates, priority, caps).

    Source: ``metadata`` on the ``cohort_findings`` slide YAML. Merged with
    ``_COHORT_FINDINGS_METADATA_DEFAULTS``.
    """
    out = copy.deepcopy(_COHORT_FINDINGS_METADATA_DEFAULTS)
    slides = _load_all_slides()
    for r in slides:
        if r.get("id") != "cohort_findings":
            continue
        md = r.get("metadata")
        if not isinstance(md, dict):
            break
        if "max_bullets" in md:
            mb = md["max_bullets"]
            if isinstance(mb, (int, float)) and int(mb) >= 1:
                out["max_bullets"] = int(mb)
        if "priority" in md and isinstance(md["priority"], list) and md["priority"]:
            out["priority"] = [str(x).strip() for x in md["priority"] if str(x).strip()]
        for key in ("singleton_list_max", "thin_list_max"):
            if key in md:
                v = md[key]
                if isinstance(v, (int, float)) and int(v) >= 1:
                    out[key] = int(v)
        if "templates" in md and isinstance(md["templates"], dict):
            for k, v in md["templates"].items():
                if isinstance(v, str) and v.strip():
                    out["templates"][k] = v.strip()
        break
    return out


def cohort_findings_min_customers_for_cross_cohort_compare() -> int:
    """Minimum customers per cohort bucket for cross-cohort comparison bullets.

    Delegates to :func:`cohort_findings_rollup_params` (``min_customers_for_cross_cohort_compare``).
    """
    return max(1, cohort_findings_rollup_params()["min_customers_for_cross_cohort_compare"])


@functools.lru_cache(maxsize=1)
def benchmarks_min_peers_for_cohort_median() -> int:
    """Minimum same-cohort peer accounts before the Peer Benchmarks / health UI uses cohort median.

    Source: ``rollup_params.min_peers_for_cohort_median`` on the ``benchmarks`` slide
    (``slides/std-07-benchmarks.yaml``). Default **3**.
    """
    m = _merge_int_rollup_params("benchmarks", {"min_peers_for_cohort_median": 3})
    return max(1, m["min_peers_for_cohort_median"])


@functools.lru_cache(maxsize=1)
def cohort_profiles_max_physical_slides() -> int:
    """Cap on physical slides emitted for the cohort profiles slide type.

    Source: ``rollup_params.max_physical_slides`` on the ``cohort_profiles`` slide
    (``slides/cohort-01-profiles.yaml``). Default **10**, clamped to 1–100.
    """
    m = _merge_int_rollup_params("cohort_profiles", {"max_physical_slides": 10})
    return max(1, min(100, m["max_physical_slides"]))


def get_slide_definition(
    slide_id: str,
    slides_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    """Return a deep copy of one slide YAML by ``id``, or None if missing.

    Used to read ``hydrate:`` and other metadata for template hydration without
    loading the full customer-filtered slide list. Passes a single ``id`` to
    :func:`_load_all_slides` so only that YAML is loaded (avoids a full-Drive
    or full-``slides/*.yaml`` walk when a folder id is set or for large trees).
    """
    for r in _load_all_slides(slides_dir, only_slide_ids={slide_id}):
        if r.get("id") == slide_id:
            return copy.deepcopy(r)
    return None


@functools.lru_cache(maxsize=1)
def hydrate_hints_by_slide_id(slides_dir: str | Path | None = None) -> dict[str, Any]:
    """Map slide ``id`` → ``hydrate`` dict for every slide YAML that defines a non-empty ``hydrate`` block.

    Used by QBR template hydrate so ``report["_hydrate_slide_hints"]`` is data-driven from ``slides/``
    instead of hardcoding one slide id in Python.
    """
    out: dict[str, Any] = {}
    for r in _load_all_slides(slides_dir):
        hid = r.get("id")
        h = r.get("hydrate")
        if not hid or not isinstance(h, dict) or not h:
            continue
        out[str(hid)] = copy.deepcopy(h)
    return out


def reset_for_tests() -> None:
    """Clear slide definition metadata caches that can leak across tests."""
    cohort_findings_rollup_params.cache_clear()
    cohort_findings_metadata.cache_clear()
    benchmarks_min_peers_for_cohort_median.cache_clear()
    cohort_profiles_max_physical_slides.cache_clear()
    hydrate_hints_by_slide_id.cache_clear()


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
