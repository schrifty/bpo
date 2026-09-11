"""Top-level ``cortex --help`` is the module docstring, grouped by task."""

from __future__ import annotations

from pathlib import Path


def test_cortex_module_help_has_organized_sections() -> None:
    import cortex

    text = (cortex.__doc__ or "").strip()
    for heading in (
        "Start here",
        "Decks",
        "Exports & data",
        "Metrics & KPIs",
        "Pendo snapshots",
        "Ops",
    ):
        assert heading in text, f"missing help section {heading!r}"
    for cmd in (
        "run --deck",
        "--customer",
        "--portfolio",
        "export-all",
        "kpi-snapshot",
        "metrics-digest",
        "run-job",
    ):
        assert cmd in text, f"missing command {cmd!r}"


def test_cortex_help_flag_prints_docstring() -> None:
    import subprocess
    import sys

    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, str(root / "cortex.py"), "--help"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "Start here" in proc.stdout
    assert "Metrics & KPIs" in proc.stdout
    assert "error:" not in proc.stderr.lower()
