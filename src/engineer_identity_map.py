"""Canonical engineer identity map for GitHub ↔ Cursor productivity joins."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import yaml

from .config_paths import GITHUB_EMAIL_ALIASES_FILE

logger = logging.getLogger("bpo")

_alias_cache: dict[str, Any] | None = None


def _casefold_email(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw.casefold() if raw else None


def load_github_email_aliases() -> tuple[dict[str, str], dict[str, str]]:
    """Return ``(raw_email → canonical, login → canonical)`` maps (casefold keys)."""
    global _alias_cache
    if _alias_cache is not None:
        return _alias_cache["emails"], _alias_cache["logins"]

    email_map: dict[str, str] = {}
    login_map: dict[str, str] = {}
    if GITHUB_EMAIL_ALIASES_FILE.is_file():
        try:
            raw = yaml.safe_load(GITHUB_EMAIL_ALIASES_FILE.read_text(encoding="utf-8")) or {}
            if isinstance(raw, dict):
                for key, val in (raw.get("emails") or {}).items():
                    canon = _casefold_email(val)
                    src = _casefold_email(key)
                    if src and canon:
                        email_map[src] = canon
                for key, val in (raw.get("logins") or {}).items():
                    canon = _casefold_email(val)
                    login = str(key or "").strip().lower()
                    if login and canon:
                        login_map[login] = canon
        except Exception as e:
            logger.warning("GitHub email aliases unreadable: %s", e)

    _alias_cache = {"emails": email_map, "logins": login_map}
    return email_map, login_map


def reset_github_alias_cache_for_tests() -> None:
    global _alias_cache
    _alias_cache = None


def roster_email_for_github_login(login: str, roster_emails: set[str]) -> str | None:
    """Map an enterprise GitHub login (e.g. ``first-last_leandna``) to a roster email."""
    login = (login or "").strip().lower()
    if not login or not roster_emails:
        return None
    roster_by_cf = {str(e).casefold(): e for e in roster_emails}
    if login.endswith("_leandna"):
        stem = login[: -len("_leandna")]
        candidate = f"{stem.replace('-', '.')}@leandna.com"
        if candidate in roster_by_cf:
            return roster_by_cf[candidate]
    for email in roster_emails:
        local = email.split("@", 1)[0].lower()
        login_stem = login.split("_", 1)[0] if login.endswith("_leandna") else login
        if login_stem.replace("-", ".") == local:
            return email
    return None


def canonicalize_email(
    raw_email: str | None,
    *,
    email_aliases: dict[str, str] | None = None,
    login_to_email: dict[str, str] | None = None,
    engineer_emails: set[str] | None = None,
) -> str | None:
    """Map a raw GitHub commit email to a canonical engineer email when possible."""
    from .github_client import parse_github_noreply_login

    email_aliases = email_aliases if email_aliases is not None else load_github_email_aliases()[0]
    login_to_email = login_to_email or {}
    engineer_emails = engineer_emails or set()

    raw = _casefold_email(raw_email)
    if not raw:
        return None
    if raw in email_aliases:
        return email_aliases[raw]
    if raw in engineer_emails:
        return raw
    login = parse_github_noreply_login(raw)
    if login:
        mapped = login_to_email.get(login) or email_aliases.get(f"{login}@users.noreply.github.com")
        if mapped:
            return mapped
    return None


def build_engineer_identity_map(
    *,
    engineer_emails: set[str] | None = None,
    cursor_members: list[dict[str, Any]] | None = None,
    github_org: str | None = None,
    github_client: Any | None = None,
    jira_client: Any | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Build canonical engineer rows for productivity correlation.

    Primary roster: Atlassian ``dev-*`` team emails (same scope as Cursor engineer slides).
    Optional enrichments: Cursor seat list, GitHub org member logins, YAML aliases.
    """
    warnings: list[str] = []
    sources: list[str] = []
    email_aliases, login_aliases = load_github_email_aliases()

    roster_emails: set[str] = set(engineer_emails or [])
    if not roster_emails:
        if jira_client is None:
            from .jira_client import JiraClient

            jira_client = JiraClient()
        from .eng_team_roster import build_engineer_audience_scope

        scope = build_engineer_audience_scope(jira_client, timeout=timeout)
        if scope.get("error"):
            return {
                "configured": False,
                "error": scope["error"],
                "warnings": warnings,
                "by_email": {},
                "login_to_email": {},
                "canonical_emails": [],
                "stats": {},
            }
        roster_emails = set(scope.get("emails") or [])
        sources.append("atlassian_teams")

    if not roster_emails:
        return {
            "configured": False,
            "error": "no engineer emails resolved from Atlassian dev-* teams",
            "warnings": warnings,
            "by_email": {},
            "login_to_email": {},
            "canonical_emails": [],
            "stats": {},
        }

    cursor_members = cursor_members or []
    if not cursor_members:
        try:
            from .cursor_client import CursorClient, cursor_configured

            if cursor_configured():
                cursor_members = CursorClient().get_team_members()
        except Exception as e:
            warnings.append(f"cursor members: {e}")
    if cursor_members:
        sources.append("cursor")
    cursor_emails = {
        _casefold_email(m.get("email"))
        for m in cursor_members
        if m.get("email")
    }
    cursor_emails.discard(None)

    login_to_email: dict[str, str] = dict(login_aliases)
    github_logins: dict[str, list[str]] = {}

    if github_client is not None and github_org:
        sources.append("github")
        try:
            for member in github_client.list_org_members(github_org):
                login = str((member.get("login") if isinstance(member, dict) else "") or "").strip().lower()
                if not login:
                    continue
                if login in login_aliases:
                    login_to_email[login] = login_aliases[login]
                    continue
                matched = roster_email_for_github_login(login, roster_emails)
                if matched:
                    login_to_email[login] = matched
        except Exception as e:
            warnings.append(f"github org members: {e}")

    by_email: dict[str, dict[str, Any]] = {}
    for email in sorted(roster_emails):
        row = {
            "canonical_email": email,
            "atlassian_engineer": True,
            "cursor_member": email in cursor_emails,
            "github_logins": [],
        }
        for login, mapped in login_to_email.items():
            if mapped == email:
                row["github_logins"].append(login)
                github_logins.setdefault(email, []).append(login)
        by_email[email] = row

    for email in cursor_emails:
        if email and email not in by_email:
            warnings.append(f"cursor seat {email} not on dev-* roster")

    stats = {
        "engineer_count": len(by_email),
        "with_cursor_seat": sum(1 for r in by_email.values() if r.get("cursor_member")),
        "with_github_login": sum(1 for r in by_email.values() if r.get("github_logins")),
    }

    return {
        "configured": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
        "github_org": github_org,
        "canonical_emails": sorted(by_email.keys()),
        "by_email": by_email,
        "login_to_email": login_to_email,
        "warnings": warnings,
        "stats": stats,
    }
