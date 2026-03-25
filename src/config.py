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
# Optional: folder ID in your Drive (shared with service account) to avoid service account quota
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
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

# Optional limits for tool output (0 = no limit, full dataset returned)
PENDO_MAX_RESULTS = int(os.environ.get("PENDO_MAX_RESULTS", "0"))
PENDO_MAX_OUTPUT_CHARS = int(os.environ.get("PENDO_MAX_OUTPUT_CHARS", "0"))

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
