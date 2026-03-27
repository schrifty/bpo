"""Tests for removing intake group Drive permission (source deck after hydrate)."""
from unittest.mock import MagicMock, patch

from src.evaluate import _remove_intake_group_permission_from_file


def test_remove_intake_group_permission_deletes_matching_email():
    drive = MagicMock()
    perm = drive.permissions.return_value
    perm.list.return_value.execute.return_value = {
        "permissions": [
            {"id": "p1", "emailAddress": "Hydrate-Deck@example.com", "type": "group"},
        ],
    }
    perm.delete.return_value.execute.return_value = {}

    n = _remove_intake_group_permission_from_file(
        drive, "file123", "hydrate-deck@example.com"
    )

    assert n == 1
    perm.delete.assert_called_once_with(
        fileId="file123",
        permissionId="p1",
        supportsAllDrives=True,
    )


def test_remove_intake_group_permission_empty_group():
    drive = MagicMock()
    assert _remove_intake_group_permission_from_file(drive, "f", "") == 0
    drive.permissions.assert_not_called()
