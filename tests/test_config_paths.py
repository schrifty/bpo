"""Project-root path resolution for env-configured files."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_resolve_path_from_project_root_relative() -> None:
    import src.config as cfg

    rel = ".env.example"
    out = cfg._resolve_path_from_project_root(rel)
    assert out == str((cfg._PROJECT_ROOT / rel).resolve())


def test_resolve_path_from_project_root_absolute() -> None:
    import src.config as cfg

    p = cfg._PROJECT_ROOT / ".env.example"
    assert cfg._resolve_path_from_project_root(str(p)) == str(p.resolve())
