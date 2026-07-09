"""Salesforce-first customer reporting groups (contracts, ARR, hierarchy).

Pendo prefixes and CS Report export names are secondary labels mapped *to* these groups.
See ``config/salesforce_reporting_rollups.yaml`` and ``.cursor/rules/salesforce-first-customer-identity.mdc``.
"""

from __future__ import annotations

import re
import threading
from pathlib import Path
from typing import Any

import yaml

from .config import logger

# Trailing parenthetical parent, e.g. "Commercial HVAC (Carrier)" -> "Carrier".
_TRAILING_PARENTHETICAL_RE = re.compile(r"\(([^()]+)\)\s*$")

_ROLLUPS_FILE = Path(__file__).resolve().parent.parent / "config" / "salesforce_reporting_rollups.yaml"
_cache_lock = threading.Lock()
_corporate_rules: list[dict[str, str]] | None = None
_label_aliases: dict[str, str] | None = None


def invalidate_salesforce_reporting_cache() -> None:
    global _corporate_rules, _label_aliases
    with _cache_lock:
        _corporate_rules = None
        _label_aliases = None


def _load_config() -> tuple[list[dict[str, str]], dict[str, str]]:
    global _corporate_rules, _label_aliases
    with _cache_lock:
        if _corporate_rules is not None and _label_aliases is not None:
            return _corporate_rules, _label_aliases

        rules: list[dict[str, str]] = []
        aliases: dict[str, str] = {}
        if _ROLLUPS_FILE.is_file():
            try:
                raw = yaml.safe_load(_ROLLUPS_FILE.read_text()) or {}
                rollups = raw.get("corporate_rollups") or {}
                if isinstance(rollups, dict):
                    for label, spec in rollups.items():
                        if not isinstance(spec, dict):
                            continue
                        corp = str(label).strip()
                        if not corp:
                            continue
                        rules.append(
                            {
                                "corporate_label": corp,
                                "parent_name_prefix": str(spec.get("parent_name_prefix") or "").strip(),
                                "name_prefix": str(spec.get("name_prefix") or "").strip(),
                            }
                        )
                raw_aliases = raw.get("label_aliases") or {}
                if isinstance(raw_aliases, dict):
                    for src, dest in raw_aliases.items():
                        s = str(src).strip()
                        d = str(dest).strip()
                        if s and d:
                            aliases[s.lower()] = d
            except Exception as e:
                logger.warning("salesforce_reporting: could not load %s: %s", _ROLLUPS_FILE, e)

        _corporate_rules = rules
        _label_aliases = aliases
        return rules, aliases


def resolve_corporate_label(label: str) -> str:
    """Map a short alias (e.g. Pendo prefix JCI) to a corporate rollup key."""
    raw = (label or "").strip()
    if not raw:
        return raw
    _, aliases = _load_config()
    return aliases.get(raw.lower(), raw)


def entity_account_division_group(account: dict[str, Any]) -> str:
    """BU / division label from SF hierarchy (ultimate parent → parent → account name)."""
    ult = (account.get("ultimate_parent_name") or "").strip()
    if ult:
        return ult
    parent = (account.get("parent_name") or "").strip()
    if parent:
        return parent
    return (account.get("Name") or "").strip() or "Unknown"


def entity_account_corporate_group(account: dict[str, Any]) -> str:
    """Corporate customer label for ARR rollups (SF-first)."""
    division = entity_account_division_group(account)
    name = (account.get("Name") or "").strip()
    parent = (account.get("parent_name") or "").strip()
    rules, _ = _load_config()

    for rule in rules:
        corp = rule["corporate_label"]
        parent_prefix = rule.get("parent_name_prefix") or ""
        name_prefix = rule.get("name_prefix") or ""
        if parent_prefix and parent.lower().startswith(parent_prefix.lower()):
            return corp
        if name_prefix and name.lower().startswith(name_prefix.lower()):
            return corp
        if parent_prefix and division.lower().startswith(parent_prefix.lower()):
            return corp

    return division


def aggregate_accounts_by_corporate_group(
    accounts: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Bucket normalized Customer Entity rows by :func:`entity_account_corporate_group`."""
    out: dict[str, list[dict[str, Any]]] = {}
    for account in accounts:
        if not isinstance(account, dict):
            continue
        group = entity_account_corporate_group(account)
        out.setdefault(group, []).append(account)
    return out


def _parenthetical_parent(account: dict[str, Any]) -> str:
    """Ultimate parent embedded as a trailing parenthetical in the entity label.

    Salesforce Customer Entity names commonly encode the parent, e.g.
    ``"Commercial HVAC (Carrier)"`` → ``"Carrier"``. Used only as a fallback when
    ``SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP`` is unset (so ``ultimate_parent_name`` is blank).
    """
    for field in ("Name", "LeanDNA_Entity_Name__c"):
        val = (account.get(field) or "").strip()
        m = _TRAILING_PARENTHETICAL_RE.search(val)
        if m:
            inner = m.group(1).strip()
            if inner:
                return inner
    return ""


def entity_account_ultimate_parent_group(account: dict[str, Any]) -> str:
    """Ultimate-parent rollup label for an account, resilient to a blank SF lookup.

    Resolution order (first hit wins):
      1. ``ultimate_parent_name`` (from ``SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP``)
      2. trailing parenthetical in the entity name (aliased via ``label_aliases``)
      3. corporate rollup group (parent/name prefix rules → division fallback)
    """
    ult = (account.get("ultimate_parent_name") or "").strip()
    if ult:
        return ult
    paren = _parenthetical_parent(account)
    if paren:
        return resolve_corporate_label(paren)
    return entity_account_corporate_group(account)


def aggregate_accounts_by_ultimate_parent(
    accounts: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Bucket normalized Customer Entity rows by :func:`entity_account_ultimate_parent_group`."""
    out: dict[str, list[dict[str, Any]]] = {}
    for account in accounts:
        if not isinstance(account, dict):
            continue
        group = entity_account_ultimate_parent_group(account)
        out.setdefault(group, []).append(account)
    return out
