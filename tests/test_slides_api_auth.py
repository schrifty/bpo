"""Google SA scope split: impersonated (DWD-authorized) vs SA identity for shared drives."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.slides_api import (
    GOOGLE_IMPERSONATED_SCOPES,
    GOOGLE_SERVICE_ACCOUNT_SCOPES,
    SCOPES,
)


def test_impersonated_scopes_match_workspace_dwd_grant() -> None:
    assert GOOGLE_IMPERSONATED_SCOPES == [
        "https://www.googleapis.com/auth/presentations",
        "https://www.googleapis.com/auth/drive",
    ]
    assert SCOPES == GOOGLE_IMPERSONATED_SCOPES


def test_service_account_scopes_are_drive_without_dwd() -> None:
    assert GOOGLE_SERVICE_ACCOUNT_SCOPES == ["https://www.googleapis.com/auth/drive"]


def test_get_service_impersonate_uses_subject(tmp_path, monkeypatch) -> None:
    import src.slides_api as sa

    key = tmp_path / "sa.json"
    key.write_text('{"project_id": "p"}', encoding="utf-8")
    monkeypatch.setattr(sa, "GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.setattr(sa, "GOOGLE_DRIVE_OWNER_EMAIL", "robot@example.com")

    creds = MagicMock()
    creds.with_quota_project.return_value = creds
    creds.with_subject.return_value = creds

    with patch.object(sa.service_account.Credentials, "from_service_account_file", return_value=creds) as from_file, patch(
        "src.slides_api.AuthorizedHttp"
    ), patch("src.slides_api.build") as build:
        sa._get_service()
    from_file.assert_called_once()
    assert from_file.call_args.kwargs["scopes"] == GOOGLE_IMPERSONATED_SCOPES
    creds.with_subject.assert_called_once_with("robot@example.com")
    assert build.call_count == 2


def test_cs_report_drive_uses_impersonated_owner_not_service_account() -> None:
    """CS Report lives on Data Exports; SA membership is Cortex Output only."""
    from src.cs_report_client import _get_drive

    with patch("src.slides_api._get_service") as get_service:
        get_service.return_value = (None, MagicMock(), None)
        _get_drive()
    get_service.assert_called_once_with(impersonate=True)


def test_get_service_without_impersonate_skips_subject(tmp_path, monkeypatch) -> None:
    import src.slides_api as sa

    key = tmp_path / "sa.json"
    key.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(sa, "GOOGLE_APPLICATION_CREDENTIALS", str(key))
    monkeypatch.setattr(sa, "GOOGLE_DRIVE_OWNER_EMAIL", "robot@example.com")

    creds = MagicMock()
    creds.with_quota_project.return_value = creds

    with patch.object(sa.service_account.Credentials, "from_service_account_file", return_value=creds) as from_file, patch(
        "src.slides_api.AuthorizedHttp"
    ), patch("src.slides_api.build"):
        sa._get_service(impersonate=False)
    assert from_file.call_args.kwargs["scopes"] == GOOGLE_SERVICE_ACCOUNT_SCOPES
    creds.with_subject.assert_not_called()
