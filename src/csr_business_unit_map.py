"""Derive authoritative business-unit assignments for Pendo sites from the CS Report.

For customers whose CS Report is delivered **split by division** (the ``customer``
column carries the division, e.g. ``Safran Cabin and Seats``), we can join each Pendo
site to the CS Report factory/entity list and inherit the division — a far stronger
signal than pattern-guessing on the Pendo site name.

This module is the *pure* join logic; :mod:`scripts.build_csr_bu_map` wraps it with
data loading (live CS Report + a Pendo site list) and emits a YAML rules fragment plus
a coverage report for review.

Confidence semantics per site (mirrors ``pendo_site_bu_map.yaml``):
  * ``high``     — a single CS Report division owns the site, or the site name
                   self-labels its division (and does not conflict with the CS Report).
  * ``inferred`` — the CS Report is ambiguous (multiple divisions match) or the site
                   name disagrees with the CS Report — needs confirmation.
  * ``none``     — no CS Report match and no self-label — falls back to the pattern
                   map / needs review.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable

__all__ = [
    "normalize_division",
    "build_csr_division_keys",
    "assign_site_business_unit",
    "assign_sites",
    "emit_bu_rules_yaml",
]

# Per-customer normalization of the CS Report ``customer`` value → our BU label.
# Keyed by lowercased Pendo prefix. Checked in order; first substring hit wins.
_DIVISION_NORMALIZERS: dict[str, list[tuple[str, str]]] = {
    "safran": [
        ("electronics and defense", "Electronics & Defense"),
        ("electrical and power", "Electrical & Power"),
        ("aerosystems", "Aerosystems"),
        ("cabin water and waste", "Cabin & Seats"),
        ("cabin and seats", "Cabin & Seats"),
    ],
}

# Site-name self-labels per customer (division phrase → BU), used to corroborate or
# conflict-check the CS Report join. Order does not matter; longest match wins.
_SELF_LABELS: dict[str, list[tuple[str, str]]] = {
    "safran": [
        ("electronics and defense", "Electronics & Defense"),
        ("electrical and power", "Electrical & Power"),
        ("aerosystems", "Aerosystems"),
        ("cabin and seats", "Cabin & Seats"),
        ("cabin", "Cabin & Seats"),
        ("seats", "Cabin & Seats"),
    ],
}


def _strip_prefix(value: str, customer_prefix: str) -> str:
    value = (value or "").strip()
    pref = (customer_prefix or "").strip()
    if pref and value.lower().startswith(pref.lower()):
        return value[len(pref):].strip()
    return value


def normalize_division(customer_prefix: str, csr_customer: str) -> str | None:
    """Map a CS Report ``customer`` value to our business-unit label.

    Uses a per-customer normalizer when configured (e.g. Safran's 7 division values
    collapse to 5 BUs); otherwise returns the residual after stripping the prefix
    (the division *is* the BU). Returns ``None`` if nothing usable remains.
    """
    residual = _strip_prefix(csr_customer, customer_prefix)
    key = (customer_prefix or "").strip().lower()
    rl = residual.lower()
    for phrase, bu in _DIVISION_NORMALIZERS.get(key, []):
        if phrase in rl:
            return bu
    if key == "safran" and rl in ("sa", ""):
        return "Other / Corporate"
    return residual or None


def _clean_location_key(raw: str) -> str:
    """Reduce a CS Report factoryName/entity to a location/name phrase for matching.

    Strips leading ERP site codes (``A1P - ``, ``2406 ``) and standalone trailing
    codes (``C48``, ``SM1``, ``0002``) so ``"Marysville CM1 Engineered Materials Main"``
    → ``"marysville engineered materials main"``.
    """
    s = (raw or "").strip()
    s = re.sub(r"^[A-Za-z0-9]{2,5}\s*-\s*", "", s)  # leading "A1P - "
    s = re.sub(r"^\d{3,5}\s+", "", s)                # leading "2406 "
    s = re.sub(r"\b([A-Za-z]{1,3}\d{1,4}|\d{3,5})\b", " ", s)  # inline codes
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def build_csr_division_keys(
    rows: Iterable[dict[str, Any]],
    customer_prefix: str,
) -> dict[str, set[str]]:
    """Map cleaned location keys → set of business units that claim them (CS Report).

    A key mapping to more than one BU is ambiguous and will be flagged, not trusted.
    """
    keys: dict[str, set[str]] = defaultdict(set)
    pref = (customer_prefix or "").strip().lower()
    for row in rows:
        csr_customer = str(row.get("customer") or "")
        # Scope strictly to the target customer's rows, else other customers'
        # factories that share a location token (e.g. another "Montreal") pollute keys.
        if pref and not csr_customer.strip().lower().startswith(pref):
            continue
        bu = normalize_division(customer_prefix, csr_customer)
        if not bu:
            continue
        for field in ("factoryName", "entity", "Entity", "site", "Site"):
            key = _clean_location_key(str(row.get(field) or ""))
            if len(key) >= 3:
                keys[key].add(bu)
    return dict(keys)


def _self_label_bu(customer_prefix: str, sitename: str) -> str | None:
    key = (customer_prefix or "").strip().lower()
    core = _strip_prefix(sitename, customer_prefix).lower()
    best: tuple[int, str] | None = None
    for phrase, bu in _SELF_LABELS.get(key, []):
        if phrase in core and (best is None or len(phrase) > best[0]):
            best = (len(phrase), bu)
    return best[1] if best else None


def assign_site_business_unit(
    sitename: str,
    customer_prefix: str,
    csr_keys: dict[str, set[str]],
) -> dict[str, Any]:
    """Assign one Pendo site to a BU using the CS Report join + name self-label.

    Returns ``{sitename, business_unit, confidence, source, matched_key, candidates}``.
    ``business_unit`` is ``None`` when nothing matched (caller falls back to patterns).
    """
    core = _strip_prefix(sitename, customer_prefix).lower()
    name_bu = _self_label_bu(customer_prefix, sitename)

    # Longest CS Report key that appears in the site core wins.
    matched_key: str | None = None
    matched_bus: set[str] = set()
    for key, bus in csr_keys.items():
        if key in core and (matched_key is None or len(key) > len(matched_key)):
            matched_key = key
            matched_bus = bus

    if matched_key and len(matched_bus) == 1:
        csr_bu = next(iter(matched_bus))
        if name_bu and name_bu != csr_bu:
            return _assign(sitename, name_bu, "inferred", "name_vs_csr_conflict",
                           matched_key, sorted(matched_bus | {name_bu}))
        return _assign(sitename, csr_bu, "high", "csr", matched_key, [csr_bu])

    if matched_key and len(matched_bus) > 1:
        if name_bu and name_bu in matched_bus:
            return _assign(sitename, name_bu, "high", "name", matched_key, sorted(matched_bus))
        return _assign(sitename, name_bu, "inferred", "csr_ambiguous",
                       matched_key, sorted(matched_bus))

    if name_bu:
        return _assign(sitename, name_bu, "high", "name", None, [name_bu])

    return _assign(sitename, None, "none", "unmatched", None, [])


def _assign(sitename, bu, confidence, source, matched_key, candidates) -> dict[str, Any]:
    return {
        "sitename": sitename,
        "business_unit": bu,
        "confidence": confidence,
        "source": source,
        "matched_key": matched_key,
        "candidates": candidates,
    }


def assign_sites(
    sitenames: Iterable[str],
    customer_prefix: str,
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assign every Pendo site to a BU. Convenience wrapper building keys once."""
    csr_keys = build_csr_division_keys(rows, customer_prefix)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for name in sitenames:
        name = (name or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(assign_site_business_unit(name, customer_prefix, csr_keys))
    return out


def emit_bu_rules_yaml(
    assignments: list[dict[str, Any]],
    customer_prefix: str,
    *,
    default_business_unit: str = "Unmapped — needs review",
) -> str:
    """Render assignments as a ``pendo_site_bu_map.yaml`` rules block for review.

    Each Pendo site name that resolved to a BU becomes an exact pattern under its BU,
    grouped and tagged with the confidence the join produced. Sites that did not match
    are listed as a comment so a reviewer can see the residual.
    """
    by_bu: dict[tuple[str, str], list[str]] = defaultdict(list)
    unmatched: list[str] = []
    for a in assignments:
        if a["business_unit"] is None:
            unmatched.append(a["sitename"])
            continue
        by_bu[(a["business_unit"], a["confidence"])].append(a["sitename"])

    lines: list[str] = [f"  {customer_prefix}:", f'    default_business_unit: "{default_business_unit}"', "    rules:"]
    for (bu, confidence), sites in sorted(by_bu.items(), key=lambda kv: (kv[0][1] != "high", kv[0][0])):
        lines.append(f'      - business_unit: "{bu}"')
        lines.append(f"        confidence: {confidence}")
        lines.append("        patterns:")
        for site in sorted(set(sites)):
            lines.append(f'          - "{site}"')
    if unmatched:
        lines.append(f"      # Unmatched ({len(unmatched)}) — fall to default / needs CS review:")
        for site in sorted(set(unmatched)):
            lines.append(f"      #   {site}")
    return "\n".join(lines) + "\n"
