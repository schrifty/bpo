"""KPI ownership model: owner field, leads config, topic packs, authz."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from src.kpi_manage_cli import run_kpi_manage_cli
from src.kpi_observation import KPIObservation
from src.kpi_owners import (
    KPIOwnershipError,
    assert_actor_may_mutate_metric,
    load_kpi_owners_config,
    reset_for_tests,
    resolve_owner_cli_value,
    validate_entry_ownership,
)
from src.kpi_service import KPIResolved, resolve_kpis
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


def test_resolve_owner_cli_me_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = load_kpi_owners_config(path=_write_owners(tmp_path))
    monkeypatch.delenv("CORTEX_KPI_ACTOR", raising=False)
    assert resolve_owner_cli_value("me", owners=cfg) == "marc.schriftman@leandna.com"
    assert (
        resolve_owner_cli_value("ME", actor="lead.eng@leandna.com", owners=cfg)
        == "lead.eng@leandna.com"
    )
    assert resolve_owner_cli_value(None, owners=cfg) is None
    assert (
        resolve_owner_cli_value(None, owners=cfg, default_to_me=True)
        == "marc.schriftman@leandna.com"
    )
    assert (
        resolve_owner_cli_value("Lead.Support@LeanDNA.com", owners=cfg)
        == "lead.support@leandna.com"
    )


def test_cli_list_defaults_to_me(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    monkeypatch.delenv("CORTEX_KPI_ACTOR", raising=False)
    rc = run_kpi_manage_cli(
        [
            "list",
            "--as-user",
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
    payload = json.loads(capsys.readouterr().out)
    assert payload["owner"] == "lead.support@leandna.com"
    assert [m["name"] for m in payload["metrics"]] == ["Beta"]


def test_cli_owners_list_metrics_defaults_to_me(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    monkeypatch.setenv("CORTEX_KPI_ACTOR", "lead.eng@leandna.com")
    rc = run_kpi_manage_cli(
        [
            "owners",
            "--list-metrics",
            "--registry",
            str(registry),
            "--owners-config",
            str(owners_path),
            "--json",
        ],
        prog="cortex kpi",
    )
    # eng lead owns nothing in fixture
    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["owner"] == "lead.eng@leandna.com"
    assert payload["metrics"] == []


def test_cli_add_owner_me_and_unauthorized_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    monkeypatch.delenv("CORTEX_KPI_ACTOR", raising=False)
    rc = run_kpi_manage_cli(
        [
            "add",
            "Lead Eng KPI",
            "--owner",
            "me",
            "--tags",
            "engineering",
            "--as-user",
            "lead.eng@leandna.com",
            "--registry",
            str(registry),
            "--owners-config",
            str(owners_path),
            "--json",
        ],
        prog="cortex kpi",
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["entry"]["owner"] == "lead.eng@leandna.com"

    rc = run_kpi_manage_cli(
        [
            "delete",
            "Alpha",
            "--yes",
            "--as-user",
            "lead.eng@leandna.com",
            "--registry",
            str(registry),
            "--owners-config",
            str(owners_path),
        ],
        prog="cortex kpi",
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "may not delete" in err


def _load_metrics_by_tag_module():
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location(
        "metrics_by_tag_under_test",
        root / "scripts" / "metrics-by-tag.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_value_cli_owner_me_and_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    mod = _load_metrics_by_tag_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "metrics-by-tag",
            "--me",
            "--as-user",
            "lead.support@leandna.com",
            "--registry",
            str(registry),
            "--owners-config",
            str(owners_path),
            "--json",
        ],
    )

    def fake_iter(**kwargs):
        assert kwargs.get("owner") == "lead.support@leandna.com"
        assert kwargs.get("registry") is not None
        yield KPIResolved(
            metric_name="Beta",
            entry={"owner": "lead.support@leandna.com", "tags": ["support"]},
            observation=KPIObservation(value=1, origin="live"),
            tags=("support",),
            automated=False,
            description="Support KPI",
            metric_id=None,
            mgmt_guidance="Keep queue healthy.",
            owner="lead.support@leandna.com",
        )

    monkeypatch.setattr(mod, "iter_resolve_kpis", fake_iter)
    monkeypatch.setattr(mod, "column_widths_for_tags", lambda *a, **k: None)
    rc = mod.main()
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 1
    assert payload[0]["metric_name"] == "Beta"
    assert payload[0]["owner"] == "lead.support@leandna.com"


def test_value_cli_bare_owner_means_me(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    mod = _load_metrics_by_tag_module()
    monkeypatch.setenv("CORTEX_KPI_ACTOR", "lead.support@leandna.com")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "metrics-by-tag",
            "--owner",
            "--registry",
            str(registry),
            "--owners-config",
            str(owners_path),
            "--json",
        ],
    )

    def fake_iter(**kwargs):
        assert kwargs.get("owner") == "lead.support@leandna.com"
        yield KPIResolved(
            metric_name="Beta",
            entry={"owner": "lead.support@leandna.com", "tags": ["support"]},
            observation=KPIObservation(value=1, origin="live"),
            tags=("support",),
            automated=False,
            description="Support KPI",
            metric_id=None,
            owner="lead.support@leandna.com",
        )

    monkeypatch.setattr(mod, "iter_resolve_kpis", fake_iter)
    monkeypatch.setattr(mod, "column_widths_for_tags", lambda *a, **k: None)
    assert mod.main() == 0
    assert json.loads(capsys.readouterr().out)[0]["metric_name"] == "Beta"


def test_add_registry_owner_me_token(tmp_path: Path) -> None:
    owners_path = _write_owners(tmp_path)
    registry = _write_registry(tmp_path)
    change = add_registry_metric(
        "From Me Token",
        path=registry,
        owner="me",
        tags=["support"],
        actor="lead.support@leandna.com",
        owners_path=owners_path,
    )
    assert change.entry is not None
    assert change.entry["owner"] == "lead.support@leandna.com"
