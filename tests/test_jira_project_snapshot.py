"""Unit tests for Jira project operational snapshot helpers."""

from datetime import timezone

from src.jira_client import JiraClient


def test_parse_jira_datetime_iso_with_plus0000():
    dt = JiraClient._parse_jira_datetime("2024-03-15T10:00:00.000+0000")
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.year == 2024 and dt.month == 3 and dt.day == 15


def test_parse_jira_datetime_z_suffix():
    dt = JiraClient._parse_jira_datetime("2024-01-01T00:00:00.000Z")
    assert dt is not None
    assert dt.tzinfo == timezone.utc


def test_parse_jira_datetime_none():
    assert JiraClient._parse_jira_datetime(None) is None
    assert JiraClient._parse_jira_datetime("") is None
