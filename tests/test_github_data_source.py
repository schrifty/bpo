"""GitHub optional datasource — preflight and QA source status."""

from unittest.mock import MagicMock, patch

from src.data_source_health import check_github
from src.github_client import check_github_api
from src.qa import QARegistry


def test_github_preflight_skipped_when_not_configured():
    with patch("src.github_client.GITHUB_TOKEN", None):
        assert check_github_api() == (True, None)
    assert check_github() == (True, None)


def test_github_preflight_ok_on_200():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "{}"
    with patch("src.github_client.GITHUB_TOKEN", "ghp_testtoken"), patch(
        "src.github_client.requests.get", return_value=mock_resp
    ) as rg:
        ok, msg = check_github_api()
    assert ok is True
    assert msg is None
    rg.assert_called_once()
    assert "Authorization" in rg.call_args[1]["headers"]


def test_github_preflight_fails_on_http_error():
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = "Bad credentials"
    with patch("src.github_client.GITHUB_TOKEN", "bad"), patch(
        "src.github_client.requests.get", return_value=mock_resp
    ):
        ok, msg = check_github_api()
    assert ok is False
    assert msg and "401" in msg


def test_qa_github_pill_unavailable_without_report_block():
    r = QARegistry()
    r.begin("Acme")
    snap = r.summary(report={"customer": "Acme"}, data_source_order=["GitHub"])
    assert snap["data_sources"]["GitHub"] == "unavailable"


def test_qa_github_pill_ok_when_report_has_github_without_error():
    r = QARegistry()
    r.begin("Acme")
    snap = r.summary(
        report={"github": {"user_login": "bot", "api": "rest"}},
        data_source_order=["GitHub"],
    )
    assert snap["data_sources"]["GitHub"] == "ok"


def test_qa_github_pill_unavailable_on_error_field():
    r = QARegistry()
    r.begin("Acme")
    snap = r.summary(
        report={"github": {"error": "rate limit"}},
        data_source_order=["GitHub"],
    )
    assert snap["data_sources"]["GitHub"] == "unavailable"
