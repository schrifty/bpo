"""Pure reproducibility reasoning helpers for evaluate/hydrate analysis."""

from __future__ import annotations

from typing import Any

from .hydrate_capabilities import AVAILABLE_DATA_KEYS, BUILDER_DESCRIPTIONS


def cache_hit_rate_line(label: str, hits: int, total: int, **extra: int) -> str:
    """Format a concise cache effectiveness summary."""
    if total <= 0:
        return f"{label}: no slides"
    pct = 100.0 * hits / total
    parts = [f"{hits}/{total} ({pct:.0f}%)"]
    for key, value in sorted(extra.items()):
        if value:
            parts.append(f"{key}={value}")
    return f"{label}: " + ", ".join(parts)


def derive_reproducibility(analysis: dict[str, Any]) -> dict[str, Any]:
    """Derive feasibility, gaps, and summary from cached data_ask vs available keys."""
    data_ask = analysis.get("data_ask") or []
    data_needed: list[dict[str, Any]] = []
    gaps: list[str] = []

    for item in data_ask:
        key = (item.get("key") or "").strip().replace(" ", "_").replace("-", "_").lower()
        if not key:
            continue
        if key.startswith("_embedded"):
            data_needed.append({
                "source": "slide",
                "fields": key,
                "available": False,
                "note": "embedded visual — cannot auto-update",
            })
            gaps.append(f"Embedded visual ({key})")
            continue
        available = key in AVAILABLE_DATA_KEYS
        data_needed.append({
            "source": "report" if available else "—",
            "fields": key,
            "available": available,
            "note": item.get("example_from_slide", ""),
        })
        if not available:
            gaps.append(key)

    n_total = len(data_ask)
    n_available = sum(1 for row in data_needed if row.get("available"))
    if n_total == 0:
        feasibility = "fully reproducible"
        summary = "Static slide; no data to fill."
    elif n_available == n_total:
        feasibility = "fully reproducible"
        summary = f"Slide asks for {n_total} data item(s); we have all of them."
    elif n_available > 0:
        feasibility = "partially reproducible"
        summary = (
            f"Slide asks for {n_total} data item(s); we have {n_available}. "
            f"Gaps: {', '.join(gaps[:5])}{'…' if len(gaps) > 5 else ''}."
        )
    else:
        feasibility = "not reproducible"
        summary = (
            f"Slide asks for {n_total} data item(s); we have none yet. "
            f"Gaps: {', '.join(gaps[:5])}{'…' if len(gaps) > 5 else ''}."
        )

    slide_type = analysis.get("slide_type") or "custom"
    has_builder = slide_type in BUILDER_DESCRIPTIONS and slide_type not in ("custom", "skip")
    if n_total == 0:
        effort_estimate = "trivial"
    elif feasibility == "fully reproducible" and has_builder:
        effort_estimate = "small"
    elif feasibility == "fully reproducible":
        effort_estimate = "medium"
    else:
        effort_estimate = "large" if len(gaps) > 3 else "medium"

    return {
        "feasibility": feasibility,
        "confidence": 100,
        "summary": summary,
        "data_needed": data_needed,
        "gaps": gaps,
        "closest_existing": slide_type if slide_type != "custom" else None,
        "effort_estimate": effort_estimate,
    }
