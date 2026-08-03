"""Tests for ECS/AWS CLI default resolution."""

from __future__ import annotations

import os

import pytest

from src import ecs_aws_defaults as defaults


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith(("CORTEX_", "BPO_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(defaults, "_should_skip_dotenv", lambda: True)


def test_default_name_prefix_reads_terraform_tfvars(monkeypatch, tmp_path):
    tf_dir = tmp_path / "infra" / "terraform"
    tf_dir.mkdir(parents=True)
    (tf_dir / "terraform.tfvars").write_text('name_prefix = "cortex"\naws_region = "us-west-2"\n', encoding="utf-8")
    monkeypatch.setattr(defaults, "_TFVARS_PATH", tf_dir / "terraform.tfvars")
    assert defaults.default_name_prefix() == "cortex"
    assert defaults.default_cluster_name() == "cortex"
    assert defaults.default_task_family() == "cortex-decks"
    assert defaults.default_region() == "us-west-2"


def test_default_name_prefix_falls_back_to_cortex(monkeypatch, tmp_path):
    missing = tmp_path / "infra" / "terraform" / "terraform.tfvars"
    monkeypatch.setattr(defaults, "_TFVARS_PATH", missing)
    assert defaults.default_name_prefix() == "cortex"
    assert defaults.default_cluster_name() == "cortex"
    assert defaults.default_task_family() == "cortex-decks"


def test_env_overrides_terraform_tfvars(monkeypatch, tmp_path):
    tf_dir = tmp_path / "infra" / "terraform"
    tf_dir.mkdir(parents=True)
    (tf_dir / "terraform.tfvars").write_text('name_prefix = "cortex"\n', encoding="utf-8")
    monkeypatch.setattr(defaults, "_TFVARS_PATH", tf_dir / "terraform.tfvars")
    monkeypatch.setenv("CORTEX_SCHEDULE_NAME_PREFIX", "custom")
    assert defaults.default_name_prefix() == "custom"


def test_cluster_not_found_error_is_actionable():
    msg = defaults.format_cluster_not_found_error(cluster="cortex", region="us-east-1")
    assert "ClusterNotFoundException" not in msg
    assert "cortex --running" in msg
    assert "CORTEX_ECS_CLUSTER" in msg
