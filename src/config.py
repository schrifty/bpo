"""BPO configuration. Pendo settings are read from environment variables."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Load .env from project root (parent of src/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _resolve_path_from_project_root(value: str | None) -> str | None:
    """Expand ``~`` and resolve relative paths against the BPO repo root."""
    if not value or not (raw := value.strip()):
        return None
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (_PROJECT_ROOT / p).resolve()
    return str(p)

# Logging: console shows INFO+ only. DEBUG only when running with debugger (LOG_LEVEL=DEBUG in launch.json)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
_bpo_logger = logging.getLogger("bpo")
_bpo_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
if not _bpo_logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setLevel(_bpo_logger.level)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"))
    _bpo_logger.addHandler(_handler)
logger = _bpo_logger

# Pendo API
PENDO_BASE_URL = os.environ.get("PENDO_BASE_URL", "https://app.pendo.io/api/v1")
PENDO_INTEGRATION_KEY = os.environ.get("PENDO_INTEGRATION_KEY")

# Google Slides API (service account JSON path; relative paths are under the repo root)
GOOGLE_APPLICATION_CREDENTIALS = _resolve_path_from_project_root(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
)
# QBR Generator folder id (Prompts, decks/, slides/, chart-data/, individual deck outputs, Output/, etc.).
# Required for hydrate/QBR and Drive-backed YAML.
GOOGLE_QBR_GENERATOR_FOLDER_ID = os.environ.get("GOOGLE_QBR_GENERATOR_FOLDER_ID", "").strip() or None
# Optional: Drive folder id where the QBR Slides template lives. If unset, the template is resolved under GOOGLE_QBR_GENERATOR_FOLDER_ID.
GOOGLE_QBR_TEMPLATE_FOLDER_ID = os.environ.get("GOOGLE_QBR_TEMPLATE_FOLDER_ID", "").strip() or None
# Exact Google Slides file name (title) for the QBR template on Drive (must match).
QBR_TEMPLATE_FILE_NAME = (
    os.environ.get("QBR_TEMPLATE_FILE_NAME", "").strip()
    or "BPO [Template] Executive Business Review [QBR]"
)
# Optional override: parent folder for `{ISO-date} - Output`; default is `<QBR Generator>/Output/`.
GOOGLE_QBR_OUTPUT_PARENT_ID = os.environ.get("GOOGLE_QBR_OUTPUT_PARENT_ID", "").strip() or None
# Portfolio / cohort: optional override for JSON snapshot folder. If unset, snapshots live under
# GOOGLE_QBR_GENERATOR_FOLDER_ID in a subfolder (see pendo_portfolio_snapshot_drive.resolve_portfolio_snapshot_folder_id).
BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID = os.environ.get("BPO_PORTFOLIO_SNAPSHOT_FOLDER_ID", "").strip() or None
# IANA zone for weekend/weekday and calendar-day logic in ``pendo_portfolio_snapshot_drive`` (Drive cache refresh).
BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ = os.environ.get("BPO_PORTFOLIO_SNAPSHOT_CALENDAR_TZ", "UTC").strip() or "UTC"
# Portfolio customer enumeration: auto | salesforce | pendo. auto uses Salesforce Customer Entity
# rollup when SF credentials are set, otherwise Pendo sitename prefixes.
_pcs = (os.environ.get("BPO_PORTFOLIO_CUSTOMER_SOURCE") or "auto").strip().lower()
BPO_PORTFOLIO_CUSTOMER_SOURCE = _pcs if _pcs else "auto"
# Optional: your email (folder owner) - transfer ownership so files count against your quota, not service account's
GOOGLE_DRIVE_OWNER_EMAIL = os.environ.get("GOOGLE_DRIVE_OWNER_EMAIL")
# Hydrate/evaluate: Google Group email (e.g. hydrate-deck@yourdomain.com). Must match Share exactly.
# Lists Slides where the group is Viewer or Editor (Drive query uses in readers OR in writers).
GOOGLE_HYDRATE_INTAKE_GROUP = os.environ.get("GOOGLE_HYDRATE_INTAKE_GROUP", "").strip() or None
# Hydrate: max slides to classify and include in the output copy. 0 = no limit. Default 10.
try:
    _hms = os.environ.get("HYDRATE_MAX_SLIDES", "10").strip()
    HYDRATE_MAX_SLIDES = max(0, int(_hms))
except ValueError:
    HYDRATE_MAX_SLIDES = 10
# After hydrate: remove GOOGLE_HYDRATE_INTAKE_GROUP from the **source** deck's sharing (Drive permission).
_rm = os.environ.get("HYDRATE_REMOVE_INTAKE_GROUP_PERMISSION", "true").strip().lower()
HYDRATE_REMOVE_INTAKE_GROUP_PERMISSION = _rm in ("1", "true", "yes", "on")
# JIRA Cloud
JIRA_URL = os.environ.get("JIRA_URL")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")

# GitHub (optional — PAT or fine-grained token for REST API preflight and future enrichment)
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip() or None
GITHUB_API_BASE_URL = os.environ.get("GITHUB_API_BASE_URL", "https://api.github.com").strip().rstrip("/") or "https://api.github.com"

# Salesforce (JWT Bearer Flow: Connected App + private key)
# SF_LOGIN_URL: https://login.salesforce.com (prod) or https://test.salesforce.com (sandbox)
SF_LOGIN_URL = os.environ.get("SF_LOGIN_URL")
SF_CONSUMER_KEY = os.environ.get("SF_CONSUMER_KEY")  # Connected App Consumer Key (Client ID)
SF_USERNAME = os.environ.get("SF_USERNAME")  # Integration user username
SF_PRIVATE_KEY = os.environ.get("SF_PRIVATE_KEY")  # PEM string (or use SF_PRIVATE_KEY_PATH)
SF_PRIVATE_KEY_PATH = os.environ.get("SF_PRIVATE_KEY_PATH")  # Path to server.key
# Optional: API name of the Account lookup to Ultimate Parent (e.g. Ultimate_Parent_Account__c).
# When set, entity Account SOQL also selects Parent + Ultimate Parent names for matching ARR.
SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP = os.environ.get("SF_ACCOUNT_ULTIMATE_PARENT_LOOKUP", "").strip()
# Customer Entity: operational / factory go-live date (date field API name on Account).
# Default matches common LeanDNA org ("Effective Date of Order"); override if you use e.g. Factory_Start_Date__c.
_sf_fs = (os.environ.get("SF_ACCOUNT_FACTORY_START_DATE_FIELD") or "").strip()
SF_ACCOUNT_FACTORY_START_DATE_FIELD = _sf_fs or "Effective_Date_of_Order__c"
# Salesforce read cache: SOQL results, global sObject describe, COUNT() totals. Default 48h.
try:
    _sf_cache_hours = float(os.environ.get("BPO_SALESFORCE_CACHE_TTL_HOURS", "48").strip())
except ValueError:
    _sf_cache_hours = 48.0
BPO_SALESFORCE_CACHE_TTL_SECONDS = max(0, int(_sf_cache_hours * 3600))
_sfc_off = os.environ.get("BPO_SALESFORCE_CACHE_DISABLED", "").strip().lower()
if _sfc_off in ("1", "true", "yes", "on"):
    BPO_SALESFORCE_CACHE_TTL_SECONDS = 0

# Jira/JSM + Salesforce: optional JSON cache in the same Drive folder as Pendo portfolio (Cache).
_idc = os.environ.get("BPO_INTEGRATION_DRIVE_CACHE_DISABLED", "").strip().lower()
BPO_INTEGRATION_DRIVE_CACHE_DISABLED = _idc in ("1", "true", "yes", "on")
_idc_fr = os.environ.get("BPO_INTEGRATION_DRIVE_CACHE_FORCE_REFRESH", "").strip().lower()
BPO_INTEGRATION_DRIVE_CACHE_FORCE_REFRESH = _idc_fr in ("1", "true", "yes", "on")

# LeanDNA Data API (optional; supply chain enrichment with Item Master Data and Shortage Trends)
# When EXECUTION_ENV is unset, read unprefixed LEANDNA_DATA_API_* (legacy).
# When EXECUTION_ENV is Staging (case-insensitive), read ST_LEANDNA_DATA_API_* only.
# When Production or CI, read PR_LEANDNA_DATA_API_* only.
# Any other non-empty EXECUTION_ENV clears LeanDNA Data API settings so calls fail until fixed.


def _leandna_data_api_execution_bucket() -> str:
    raw = (os.environ.get("EXECUTION_ENV") or "").strip()
    if not raw:
        return "legacy"
    low = raw.lower()
    if low == "staging":
        return "staging"
    if low in ("production", "ci", "production (ci)", "production(ci)", "production/ci"):
        return "production"
    return "none"


BPO_LEANDNA_DATA_API_EXECUTION_BUCKET = _leandna_data_api_execution_bucket()

if BPO_LEANDNA_DATA_API_EXECUTION_BUCKET == "legacy":
    LEANDNA_DATA_API_BASE_URL = os.environ.get("LEANDNA_DATA_API_BASE_URL", "https://app.leandna.com/api").rstrip("/")
    LEANDNA_DATA_API_BEARER_TOKEN = os.environ.get("LEANDNA_DATA_API_BEARER_TOKEN")
    LEANDNA_DATA_API_COOKIE = (os.environ.get("LEANDNA_DATA_API_COOKIE") or "").strip()
    LEANDNA_DATA_API_ORIGIN = (os.environ.get("LEANDNA_DATA_API_ORIGIN") or "").strip()
    LEANDNA_DATA_API_REFERER = (os.environ.get("LEANDNA_DATA_API_REFERER") or "").strip()
elif BPO_LEANDNA_DATA_API_EXECUTION_BUCKET == "staging":
    _LD_PRE = "ST_"
    LEANDNA_DATA_API_BASE_URL = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_BASE_URL") or "").strip().rstrip("/")
    LEANDNA_DATA_API_BEARER_TOKEN = os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_BEARER_TOKEN")
    LEANDNA_DATA_API_COOKIE = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_COOKIE") or "").strip()
    LEANDNA_DATA_API_ORIGIN = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_ORIGIN") or "").strip()
    LEANDNA_DATA_API_REFERER = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_REFERER") or "").strip()
elif BPO_LEANDNA_DATA_API_EXECUTION_BUCKET == "production":
    _LD_PRE = "PR_"
    LEANDNA_DATA_API_BASE_URL = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_BASE_URL") or "").strip().rstrip("/")
    LEANDNA_DATA_API_BEARER_TOKEN = os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_BEARER_TOKEN")
    LEANDNA_DATA_API_COOKIE = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_COOKIE") or "").strip()
    LEANDNA_DATA_API_ORIGIN = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_ORIGIN") or "").strip()
    LEANDNA_DATA_API_REFERER = (os.environ.get(f"{_LD_PRE}LEANDNA_DATA_API_REFERER") or "").strip()
else:
    LEANDNA_DATA_API_BASE_URL = ""
    LEANDNA_DATA_API_BEARER_TOKEN = None
    LEANDNA_DATA_API_COOKIE = ""
    LEANDNA_DATA_API_ORIGIN = ""
    LEANDNA_DATA_API_REFERER = ""


def resolve_leandna_data_api_base_url() -> str:
    """Return the Data API base URL (no trailing slash) or raise if misconfigured.

    Legacy mode (empty configured base) falls back to production host. Staging, production,
    and unknown ``EXECUTION_ENV`` buckets require an explicit base URL.
    """
    raw = (LEANDNA_DATA_API_BASE_URL or "").strip().rstrip("/")
    if raw:
        return raw
    if BPO_LEANDNA_DATA_API_EXECUTION_BUCKET == "legacy":
        return "https://app.leandna.com/api".rstrip("/")
    raise ValueError(
        "LEANDNA_DATA_API_BASE_URL is not set for this EXECUTION_ENV. "
        "Set ST_LEANDNA_DATA_API_BASE_URL when EXECUTION_ENV=Staging, "
        "PR_LEANDNA_DATA_API_BASE_URL when EXECUTION_ENV is Production or CI, "
        "or unset EXECUTION_ENV to use LEANDNA_DATA_API_BASE_URL."
    )


def execution_env_disallows_http_mutations() -> bool:
    """True when ``EXECUTION_ENV`` is Production or CI (``PR_*`` credential bucket)."""
    return BPO_LEANDNA_DATA_API_EXECUTION_BUCKET == "production"


def _production_http_mutations_explicitly_allowed() -> bool:
    raw = os.environ.get("BPO_ALLOW_PRODUCTION_MUTATIONS", "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def leandna_http_mutations_allowed() -> bool:
    """Whether LeanDNA Data API POST/PUT/DELETE are permitted for the current process."""
    if not execution_env_disallows_http_mutations():
        return True
    return _production_http_mutations_explicitly_allowed()


def leandna_http_mutation_blocked_envelope(*, method: str, path: str = "") -> dict[str, Any] | None:
    """Return a tool/client error envelope when production mode blocks mutations; else ``None``."""
    if leandna_http_mutations_allowed():
        if execution_env_disallows_http_mutations() and _production_http_mutations_explicitly_allowed():
            logger.warning(
                "BPO_ALLOW_PRODUCTION_MUTATIONS is set; allowing LeanDNA %s %s despite production EXECUTION_ENV",
                method,
                path or "(no path yet)",
            )
        return None
    env_label = (os.environ.get("EXECUTION_ENV") or "").strip() or "Production/CI"
    return {
        "ok": False,
        "error": (
            "LeanDNA Data API mutations (POST, PUT, DELETE) are disabled when "
            f"EXECUTION_ENV is {env_label!r} (production / PR_* credentials)."
        ),
        "hint": (
            "Use EXECUTION_ENV=Staging for writes, unset EXECUTION_ENV for legacy dev, "
            "or set BPO_ALLOW_PRODUCTION_MUTATIONS=true to opt in explicitly."
        ),
        "method": method,
        "path": path,
    }


try:
    _ldna_cache_hours = int(os.environ.get("LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS", "24").strip())
    LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS = max(1, min(168, _ldna_cache_hours))  # 1h-7d range
except ValueError:
    LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS = 24
try:
    _ldna_shortage_cache_hours = int(os.environ.get("LEANDNA_SHORTAGE_CACHE_TTL_HOURS", "12").strip())
    LEANDNA_SHORTAGE_CACHE_TTL_HOURS = max(1, min(48, _ldna_shortage_cache_hours))  # 1h-48h range
except ValueError:
    LEANDNA_SHORTAGE_CACHE_TTL_HOURS = 12
try:
    _ldna_projects_cache_hours = int(os.environ.get("LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS", "24").strip())
    LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS = max(1, min(168, _ldna_projects_cache_hours))  # 1h-7d range
except ValueError:
    LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS = 24

# Optional limits for tool output (0 = no limit, full dataset returned)
PENDO_MAX_RESULTS = int(os.environ.get("PENDO_MAX_RESULTS", "0"))
PENDO_MAX_OUTPUT_CHARS = int(os.environ.get("PENDO_MAX_OUTPUT_CHARS", "0"))
# Pendo read/preload caches: in-process slices plus Drive JSON preload/portfolio snapshots.
try:
    _pendo_cache_seconds = int(os.environ.get("BPO_PENDO_CACHE_TTL_SECONDS", "120").strip())
except ValueError:
    _pendo_cache_seconds = 120
BPO_PENDO_CACHE_TTL_SECONDS = max(0, _pendo_cache_seconds)
_pendo_cache_disabled = os.environ.get("BPO_PENDO_CACHE_DISABLED", "").strip().lower()
if _pendo_cache_disabled in ("1", "true", "yes", "on"):
    BPO_PENDO_CACHE_TTL_SECONDS = 0

# Feature Adoption slide: half-over-half usage narrative (extra Pendo aggregations). Off by default — disable by unsetting or false.
_fai = os.environ.get("BPO_FEATURE_ADOPTION_INSIGHTS", "").strip().lower()
FEATURE_ADOPTION_INSIGHTS = _fai in ("1", "true", "yes", "on")

# Notable Signals: optional LLM pass to prioritize / merge heuristic + cross-source lines (after Phase 1 rules).
_sslm = os.environ.get("BPO_SIGNALS_LLM", "").strip().lower()
BPO_SIGNALS_LLM = _sslm in ("1", "true", "yes", "on")
try:
    _sslm_max = int(os.environ.get("BPO_SIGNALS_LLM_MAX_ITEMS", "10").strip())
    BPO_SIGNALS_LLM_MAX_ITEMS = max(3, min(15, _sslm_max))
except ValueError:
    BPO_SIGNALS_LLM_MAX_ITEMS = 10

# Phase 3: Manifest + executive-summary signals slide YAML → editorial guidance for the signals LLM (QBR only passes these).
try:
    BPO_SIGNALS_LLM_MANIFEST_MAX_CHARS = max(
        500,
        min(12000, int(os.environ.get("BPO_SIGNALS_LLM_MANIFEST_MAX_CHARS", "8000").strip())),
    )
except ValueError:
    BPO_SIGNALS_LLM_MANIFEST_MAX_CHARS = 8000
try:
    BPO_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS = max(
        200,
        min(8000, int(os.environ.get("BPO_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS", "2500").strip())),
    )
except ValueError:
    BPO_SIGNALS_LLM_SLIDE_PROMPT_MAX_CHARS = 2500
_sed = os.environ.get("BPO_SIGNALS_LLM_EDITORIAL", "true").strip().lower()
BPO_SIGNALS_LLM_EDITORIAL = _sed not in ("0", "false", "no", "off")
_sdp = os.environ.get("BPO_SIGNALS_LLM_DECK_PROMPT", "true").strip().lower()
BPO_SIGNALS_LLM_DECK_PROMPT = _sdp not in ("0", "false", "no", "off")

# Notable Signals: Pendo visitor-window comparisons for trend banner + LLM (extra aggregate calls when on).
_std = os.environ.get("BPO_SIGNALS_TRENDS", "true").strip().lower()
BPO_SIGNALS_TRENDS = _std not in ("0", "false", "no", "off")
# Prior N-day window = second full-org visitor pull (heavy); off by default — enable for QoQ-style deltas.
_stp = os.environ.get("BPO_SIGNALS_TRENDS_PRIOR_PERIOD", "false").strip().lower()
BPO_SIGNALS_TRENDS_PRIOR_PERIOD = _stp in ("1", "true", "yes", "on")
try:
    BPO_SIGNALS_TRENDS_TIMEOUT = max(
        15,
        min(180, int(os.environ.get("BPO_SIGNALS_TRENDS_TIMEOUT", "75").strip())),
    )
except ValueError:
    BPO_SIGNALS_TRENDS_TIMEOUT = 75
_stw = os.environ.get("BPO_SIGNALS_TRENDS_WOW", "true").strip().lower()
BPO_SIGNALS_TRENDS_WOW = _stw not in ("0", "false", "no", "off")
_stm = os.environ.get("BPO_SIGNALS_TRENDS_MOM", "false").strip().lower()
BPO_SIGNALS_TRENDS_MOM = _stm in ("1", "true", "yes", "on")
_sty = os.environ.get("BPO_SIGNALS_TRENDS_YOY", "false").strip().lower()
BPO_SIGNALS_TRENDS_YOY = _sty in ("1", "true", "yes", "on")

# LLM provider — set LLM_PROVIDER=gemini or LLM_PROVIDER=openai in .env.
# Defaults to gemini if GEMINI_API_KEY is present, otherwise openai.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
_default_provider = "gemini" if GEMINI_API_KEY else "openai"
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", _default_provider).lower()

_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

if LLM_PROVIDER == "gemini":
    LLM_MODEL = "gemini-2.5-flash"
    LLM_MODEL_FAST = "gemini-2.5-flash"
else:
    LLM_MODEL = "gpt-4o"
    LLM_MODEL_FAST = "gpt-4o-mini"


def llm_client():
    """Return an OpenAI-SDK client for the configured LLM provider."""
    from openai import OpenAI
    if LLM_PROVIDER == "gemini":
        if not GEMINI_API_KEY:
            raise RuntimeError("LLM_PROVIDER=gemini but GEMINI_API_KEY is not set")
        return OpenAI(api_key=GEMINI_API_KEY, base_url=_GEMINI_BASE_URL)
    if not OPENAI_API_KEY:
        raise RuntimeError("LLM_PROVIDER=openai but OPENAI_API_KEY is not set")
    return OpenAI(api_key=OPENAI_API_KEY)
