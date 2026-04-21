"""BPO configuration. Pendo settings are read from environment variables."""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (parent of src/)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

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

# Google Slides API (service account JSON path)
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# QBR Generator folder id (Prompts, decks/, slides/, chart-data/, Decks-{date}/). Required for hydrate/QBR and Drive-backed YAML.
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
# Salesforce read cache: SOQL results, global sObject describe, COUNT() totals. Default 48h.
try:
    _sf_cache_hours = float(os.environ.get("BPO_SALESFORCE_CACHE_TTL_HOURS", "48").strip())
except ValueError:
    _sf_cache_hours = 48.0
BPO_SALESFORCE_CACHE_TTL_SECONDS = max(0, int(_sf_cache_hours * 3600))
_sfc_off = os.environ.get("BPO_SALESFORCE_CACHE_DISABLED", "").strip().lower()
if _sfc_off in ("1", "true", "yes", "on"):
    BPO_SALESFORCE_CACHE_TTL_SECONDS = 0
_sfc_fr = os.environ.get("BPO_SALESFORCE_CACHE_FORCE_REFRESH", "").strip().lower()
BPO_SALESFORCE_CACHE_FORCE_REFRESH = _sfc_fr in ("1", "true", "yes", "on")

# LeanDNA Data API (optional; supply chain enrichment with Item Master Data and Shortage Trends)
LEANDNA_DATA_API_BASE_URL = os.environ.get("LEANDNA_DATA_API_BASE_URL", "https://app.leandna.com/api").rstrip("/")
LEANDNA_DATA_API_BEARER_TOKEN = os.environ.get("LEANDNA_DATA_API_BEARER_TOKEN")  # required for LeanDNA integration
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
