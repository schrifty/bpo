"""Add / edit / delete KPI definitions in config/my-metrics.yaml."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from src.kpi_manage_cli import run_kpi_manage_cli
from src.metrics_registry import get_registry_metric, load_metrics_registry
from src.metrics_registry_write import (
    MetricsRegistryWriteError,
    add_registry_metric,
    apply_metric_fields,
    delete_registry_metric,
    edit_registry_metric,
    format_metric_block,
)

_HEADER = """# LeanDNA metric registry — test fixture
#
# Keep this comment.

metrics:
  # Existing Alpha
  "Alpha":
    description: First metric.
    metric-id: 10
    metric-generator: gen_a
    tags: [engineering]
    target: 80
    direction: higher

  "Beta":
    description: null
    metric-id: null
    metric-generator: null
    tags: [support]
"""


def _write_registry(tmp_path: Path) -> Path:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(_HEADER, encoding="utf-8")
    return path


def test_get_registry_metric(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    reg = load_metrics_registry(path=path)
    found = get_registry_metric("Alpha", registry=reg)
    assert found is not None
    assert found[0] == "Alpha"
    assert found[1]["metric-id"] == 10
    assert get_registry_metric("Missing", registry=reg) is None
    assert get_registry_metric("  ", registry=reg) is None


def test_format_metric_block_matches_house_style() -> None:
    block = format_metric_block(
        "Sprint Delivery %",
        {
            "description": "Unweighted mean of per-board delivery % for the latest closed sprint.",
            "metric-id": None,
            "metric-generator": "get_sprint_delivery_by_team",
            "tags": ["engineering"],
            "unit": "percent",
            "target": 80,
            "direction": "higher",
        },
    )
    assert block.startswith('  # Sprint Delivery %\n  "Sprint Delivery %":\n')
    assert "    metric-id: null\n" in block
    assert "    mgmt_guidance: null\n" in block
    assert "    metric-generator: get_sprint_delivery_by_team\n" in block
    assert "    tags: [engineering]\n" in block
    assert "    unit: percent\n" in block
    assert "    target: 80\n" in block
    assert "    direction: higher\n" in block


def test_add_edit_delete_preserves_unrelated_comments(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    add_registry_metric(
        "Gamma KPI",
        path=path,
        description="New row",
        tags=["ops", "Engineering"],
        generator="get_kpi_automation_pct",
        target=90,
        direction="higher",
        unit="percent",
    )
    text = path.read_text(encoding="utf-8")
    assert "# LeanDNA metric registry — test fixture" in text
    assert "  # Existing Alpha" in text
    assert '  "Gamma KPI":' in text
    assert "    tags: [ops, engineering]" in text

    edit_registry_metric(
        "Alpha",
        path=path,
        add_tags=["ai"],
        target=85,
        direction="higher",
    )
    text = path.read_text(encoding="utf-8")
    assert "  # Existing Alpha" in text
    alpha = get_registry_metric("Alpha", registry=load_metrics_registry(path=path))
    assert alpha is not None
    assert alpha[1]["target"] == 85
    assert "ai" in alpha[1]["tags"]

    delete_registry_metric("Beta", path=path)
    text = path.read_text(encoding="utf-8")
    assert '"Beta"' not in text
    assert '"Alpha"' in text
    assert '"Gamma KPI"' in text
    assert "# LeanDNA metric registry — test fixture" in text


def test_add_duplicate_and_missing_edit(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    with pytest.raises(MetricsRegistryWriteError, match="already exists"):
        add_registry_metric("Alpha", path=path)
    with pytest.raises(MetricsRegistryWriteError, match="not found"):
        edit_registry_metric("Nope", path=path, description="x")
    with pytest.raises(MetricsRegistryWriteError, match="not found"):
        delete_registry_metric("Nope", path=path)


def test_target_requires_direction(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    with pytest.raises(MetricsRegistryWriteError, match="direction missing"):
        add_registry_metric("Needs Direction", path=path, target=10)
    with pytest.raises(MetricsRegistryWriteError, match="direction missing"):
        edit_registry_metric("Beta", path=path, target=5)


def test_rename_and_dry_run(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    original = path.read_text(encoding="utf-8")
    change = add_registry_metric("Dry KPI", path=path, tags=["ops"], dry_run=True)
    assert change.dry_run is True
    assert path.read_text(encoding="utf-8") == original

    edit_registry_metric("Alpha", path=path, new_name="Alpha Renamed")
    reg = load_metrics_registry(path=path)
    assert get_registry_metric("Alpha", registry=reg) is None
    renamed = get_registry_metric("Alpha Renamed", registry=reg)
    assert renamed is not None
    assert renamed[1]["metric-id"] == 10
    text = path.read_text(encoding="utf-8")
    assert '  "Alpha Renamed":' in text
    assert "  # Existing Alpha" in text


def test_apply_metric_fields_tag_mutations() -> None:
    base = {"tags": ["engineering"], "description": "d", "metric-id": None, "metric-generator": None}
    updated = apply_metric_fields(base, add_tags=["AI"], remove_tags=["engineering"])
    assert updated["tags"] == ["ai"]
    cleared = apply_metric_fields(updated, clear_tags=True)
    assert cleared["tags"] == []


def test_cli_add_edit_delete_show(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    rc = run_kpi_manage_cli(
        [
            "add",
            "CLI KPI",
            "--tags",
            "engineering,ai",
            "--description",
            "From CLI",
            "--registry",
            str(path),
            "--json",
        ],
        prog="cortex kpi",
    )
    assert rc == 0
    found = get_registry_metric("CLI KPI", registry=load_metrics_registry(path=path))
    assert found is not None
    assert found[1]["description"] == "From CLI"

    rc = run_kpi_manage_cli(
        ["edit", "CLI KPI", "--add-tag", "ops", "--target", "12", "--direction", "lower", "--registry", str(path)],
        prog="cortex kpi",
    )
    assert rc == 0
    found = get_registry_metric("CLI KPI", registry=load_metrics_registry(path=path))
    assert found is not None
    assert "ops" in found[1]["tags"]
    assert found[1]["target"] == 12.0

    rc = run_kpi_manage_cli(["show", "CLI KPI", "--registry", str(path), "--json"], prog="cortex kpi")
    assert rc == 0

    rc = run_kpi_manage_cli(
        ["delete", "CLI KPI", "--yes", "--registry", str(path)],
        prog="cortex kpi",
    )
    assert rc == 0
    assert get_registry_metric("CLI KPI", registry=load_metrics_registry(path=path)) is None


def test_cli_delete_requires_yes_without_tty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write_registry(tmp_path)
    monkeypatch.setattr("src.kpi_manage_cli.sys.stdin.isatty", lambda: False)
    rc = run_kpi_manage_cli(["delete", "Alpha", "--registry", str(path)], prog="cortex kpi")
    assert rc == 1
    assert get_registry_metric("Alpha", registry=load_metrics_registry(path=path)) is not None


def test_cli_help_lists_subcommands() -> None:
    with pytest.raises(SystemExit) as caught:
        run_kpi_manage_cli(["--help"], prog="cortex kpi")
    assert caught.value.code == 0


def test_special_name_roundtrip(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    add_registry_metric("% AI-Assisted PRs (Test)", path=path, tags=["ai"])
    edit_registry_metric("% AI-Assisted PRs (Test)", path=path, description="Share of PRs")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "% AI-Assisted PRs (Test)" in loaded["metrics"]
    delete_registry_metric("% AI-Assisted PRs (Test)", path=path)
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert "% AI-Assisted PRs (Test)" not in loaded["metrics"]


def test_every_catalog_kpi_has_mgmt_guidance() -> None:
    from src.metrics_registry import iter_all_metrics, registry_metric_mgmt_guidance

    missing = [
        name
        for name, entry in iter_all_metrics()
        if not registry_metric_mgmt_guidance(entry)
    ]
    assert missing == []


def test_catalog_mgmt_guidance_is_at_most_two_sentences() -> None:
    import re

    from src.metrics_registry import iter_all_metrics, registry_metric_mgmt_guidance

    too_long: list[tuple[str, int]] = []
    for name, entry in iter_all_metrics():
        text = registry_metric_mgmt_guidance(entry) or ""
        sentences = [s for s in re.split(r"[.!?]+(?:\s+|$)", text.strip()) if s]
        if len(sentences) > 2:
            too_long.append((name, len(sentences)))
    assert too_long == []


def test_edit_sets_mgmt_guidance(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    edit_registry_metric(
        "Alpha",
        path=path,
        mgmt_guidance="Hit the target. Investigate misses.",
    )
    alpha = get_registry_metric("Alpha", registry=load_metrics_registry(path=path))
    assert alpha is not None
    assert alpha[1]["mgmt_guidance"] == "Hit the target. Investigate misses."
