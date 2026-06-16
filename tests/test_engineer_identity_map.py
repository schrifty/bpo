"""Tests for engineer identity map (GitHub ↔ Cursor join)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.engineer_identity_map import (
    build_engineer_identity_map,
    canonicalize_email,
    load_github_email_aliases,
    reset_github_alias_cache_for_tests,
)


def setup_function() -> None:
    reset_github_alias_cache_for_tests()


def test_canonicalize_email_uses_roster_and_aliases():
    aliases = {"schrifty@gmail.com": "dev@leandna.com"}
    assert canonicalize_email(
        "schrifty@gmail.com",
        email_aliases=aliases,
        engineer_emails={"dev@leandna.com"},
    ) == "dev@leandna.com"
    assert canonicalize_email(
        "dev@leandna.com",
        email_aliases=aliases,
        engineer_emails={"dev@leandna.com"},
    ) == "dev@leandna.com"


def test_canonicalize_noreply_via_login_map():
    assert canonicalize_email(
        "123+alice@users.noreply.github.com",
        login_to_email={"alice": "alice@leandna.com"},
        engineer_emails={"alice@leandna.com"},
    ) == "alice@leandna.com"


def test_build_identity_map_from_atlassian_scope():
    jira = MagicMock()
    gh = MagicMock()
    gh.list_org_members.return_value = [{"login": "alice"}]

    scope = {
        "error": None,
        "emails": {"alice@leandna.com"},
        "headcount": 1,
    }
    with patch("src.eng_team_roster.build_engineer_audience_scope", return_value=scope), patch(
        "src.engineer_identity_map.load_github_email_aliases",
        return_value=({}, {"alice": "alice@leandna.com"}),
    ), patch("src.cursor_client.cursor_configured", return_value=False):
        identity = build_engineer_identity_map(
            jira_client=jira,
            github_client=gh,
            github_org="leandna-apex",
        )

    assert identity["configured"] is True
    assert "alice@leandna.com" in identity["by_email"]
    assert identity["by_email"]["alice@leandna.com"]["github_logins"] == ["alice"]


def test_load_aliases_empty_by_default():
    emails, logins = load_github_email_aliases()
    assert isinstance(emails, dict)
    assert isinstance(logins, dict)
