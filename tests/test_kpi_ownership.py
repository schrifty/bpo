"""KPI ownership model: owner field, leads config, topic packs, authz."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.kpi_manage_cli import run_kpi_manage_cli
from src.kpi_owners import (
    KPIOwnershipError,
    assert_actor_may_mutate_metric,
    load_kpi_owners_config,
    reset_for_tests,
    validate_entry_ownership,
)
from src.kpi_service import resolve_kpis
from src.metrics_registry import (
    all_registry_owners,
    iter_all_metrics,
    iter_metrics_by_owner,
    load_metrics_registry,
    registry_metric_owner,
)
from src.metrics_registry_write import (
    MetricsRegistryWriteError,
    add_registry_metric,
    edit_registry_metric,
    public_metric_entry,
)

_OWNERS_YAML = """
catalog_admin: marc.schriftman@leandna.com
leads:
  - email: lead.eng@leandna.com
    display_name: Eng Lead
    packs: [engineering]
  - email: lead.support@leandna.com
    display_name: Support Lead
    packs: [support]
topic_packs:
  engineering:
    description: Eng pack
    required_any_tags: [engineering]
  support:
    description: Support pack
    required_any_tags: [support]
  akkr:
    description: AKKR pack
    required_any_tags: [akkr]
"""

_REGISTRY = """
metrics:
  "Alpha":
    description: Eng KPI
    mgmt_guidance: Hit it.
    owner: marc.schriftman@leandna.com
    metric-id: 1
    metric-generator: gen_a
    tags: [engineering]
  "Beta":
    description: Support KPI
    mgmt_guidance: Keep queue healthy.
    owner: lead.support@leandna.com
    metric-id: null
    metric-generator: null
    tags: [support]
"""


@pytest.fixture(autouse=True)
def _clear_owners_cache() -> None:
    reset_for_tests()
    yield
    reset_for_tests()


def _write_owners(tmp_path: Path) -> Path:
    path = tmp_path / "kpi_owners.yaml"
    path.write_text(_OWNERS_YAML.strip() + "\n", encoding="utf-8")
    return path


def _write_registry(tmp_path: Path) -> Path:
    path = tmp_path / "my-metrics.yaml"
    path.write_text(_REGISTRY.strip() + "\n", encoding="utf-8")
    return path


def test_repo_catalog_every_metric_has_owner() -> None:
    missing = [
        name
        for name, entry in iter_all_metrics()
        if not registry_metric_owner(entry)
    ]
    assert missing == []
    owners = all_registry_owners()
    assert owners
    assert owners[0][0] == "marc.schriftman@leandna.com"
    assert owners[0][1] == len(list(iter_all_metrics()))


def test_iter_metrics_by_owner(tmp_path: Path) -> None:
    path = _write_registry(tmp_path)
    reg = load_metrics_registry(path=path)
    marc = iter_metrics_by_owner("Marc.Schriftman@LeanDNA.com", registry=reg)
    assert [n for n, _ in marc] == ["Alpha"]
    support = iter_metrics_by_owner("lead.support@leandna.com", registry=reg)
    assert [n for n, _ in support] == ["Beta"]
    assert iter_metrics_by_owner("nobody@leandna.com", registry=reg) == []


def test_load_kpi_owners_and_pack_validation(tmp_path: Path) -> None:
    owners_path = _write_owners(tmp_path)
    cfg = load_kpi_owners_config(path=owners_path)
    assert cfg.catalog_admin == "marc.schriftman@leandna.com"
    assert {lead.email for lead in cfg.leads} == {
        "lead.eng@leandna.com",
        "lead.support@leandna.com",
    }
    assert "engineering" in cfg.topic_packs
    assert validate_entry_ownership(
        {"owner": "lead.eng@leandna.com", "tags": ["engineering", "ai"]},
        owners=cfg,
    ) is None
    err = validate_entry_ownership(
        {"owner": "lead.eng@leandna.com", "tags": ["support"]},
        owners=cfg,
    )
    assert err is not None and "packs" in err
    err = validate_entry_ownership({"owner": "stranger@x.com", "tags": ["engineering"]}, owners=cfg)
    assert err is not None and "unknown" in err


def test_unauthorized_edit_fails_loud(tmp_path: Path) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    with pytest.raises(MetricsRegistryWriteError, match="may not edit"):
        edit_registry_metric(
            "Alpha",
            path=registry,
            description="Nope",
            actor="lead.support@leandna.com",
            owners_path=owners_path,
        )
    with pytest.raises(MetricsRegistryWriteError, match="only create KPIs owned by themselves"):
        add_registry_metric(
            "Lead Attempt",
            path=registry,
            owner="marc.schriftman@leandna.com",
            tags=["engineering"],
            actor="lead.eng@leandna.com",
            owners_path=owners_path,
        )


def test_lead_can_edit_own_and_set_generator(tmp_path: Path) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    change = edit_registry_metric(
        "Beta",
        path=registry,
        generator="get_open_help",
        metric_id=99,
        actor="lead.support@leandna.com",
        owners_path=owners_path,
    )
    assert change.entry is not None
    assert change.entry["metric-generator"] == "get_open_help"
    assert change.entry["metric-id"] == 99
    assert change.entry["owner"] == "lead.support@leandna.com"


def test_edit_preserves_owner_when_unset(tmp_path: Path) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    change = edit_registry_metric(
        "Alpha",
        path=registry,
        description="Updated",
        actor="marc.schriftman@leandna.com",
        owners_path=owners_path,
    )
    assert change.entry is not None
    assert change.entry["owner"] == "marc.schriftman@leandna.com"
    assert "owner" in public_metric_entry(change.entry)


def test_invalid_owner_on_add_fails_loud(tmp_path: Path) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    with pytest.raises(MetricsRegistryWriteError, match="unknown KPI owner"):
        add_registry_metric(
            "Ghost",
            path=registry,
            owner="ghost@leandna.com",
            tags=["engineering"],
            actor="marc.schriftman@leandna.com",
            owners_path=owners_path,
        )


def test_cli_owners_list_metrics(tmp_path: Path) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    rc = run_kpi_manage_cli(
        [
            "owners",
            "--list-metrics",
            "--owner",
            "lead.support@leandna.com",
            "--registry",
            str(registry),
            "--owners-config",
            str(owners_path),
            "--json",
        ],
        prog="cortex kpi",
    )
    assert rc == 0


def test_resolve_kpis_by_owner(tmp_path: Path) -> None:
    registry = _write_registry(tmp_path)
    reg = load_metrics_registry(path=registry)
    rows = resolve_kpis(owner="lead.support@leandna.com", registry=reg, mode="live")
    assert [r.metric_name for r in rows] == ["Beta"]
    assert rows[0].owner == "lead.support@leandna.com"


def test_assert_actor_unknown_fails_loud(tmp_path: Path) -> None:
    cfg = load_kpi_owners_config(path=_write_owners(tmp_path))
    with pytest.raises(KPIOwnershipError, match="unauthorized KPI actor"):
        assert_actor_may_mutate_metric(
            "stranger@leandna.com",
            existing_owner="marc.schriftman@leandna.com",
            new_owner="marc.schriftman@leandna.com",
            action="edit",
            owners=cfg,
        )


def test_repo_owners_config_loads() -> None:
    cfg = load_kpi_owners_config()
    assert cfg.catalog_admin == "marc.schriftman@leandna.com"
    assert "engineering" in cfg.topic_packs
    assert "support" in cfg.topic_packs
    assert "akkr" in cfg.topic_packs
