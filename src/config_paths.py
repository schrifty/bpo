"""Canonical paths for Cortex YAML config (aliases, cohorts, portfolio maps)."""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = _REPO_ROOT / "config"

COHORTS_FILE = CONFIG_DIR / "cohorts.yaml"
METRICS_FILE = CONFIG_DIR / "my-metrics.yaml"
JSM_ORGANIZATION_ALIASES_FILE = CONFIG_DIR / "jsm_organization_aliases.yaml"
CS_REPORT_CUSTOMER_ALIASES_FILE = CONFIG_DIR / "cs_report_customer_aliases.yaml"
SLACK_CUSTOMER_ALIASES_FILE = CONFIG_DIR / "slack_customer_aliases.yaml"
CUSTOMER_IDENTITY_MAP_FILE = CONFIG_DIR / "customer_identity_map.yaml"
SF_PORTFOLIO_PENDO_ALIASES_FILE = CONFIG_DIR / "sf_portfolio_pendo_aliases.yaml"
PENDO_ORPHANS_FILE = CONFIG_DIR / "pendo_orphans.yaml"
GITHUB_EMAIL_ALIASES_FILE = CONFIG_DIR / "github_email_aliases.yaml"
GITHUB_REPO_DENYLIST_FILE = CONFIG_DIR / "github_repo_denylist.yaml"
