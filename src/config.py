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
# JIRA Cloud
JIRA_URL = os.environ.get("JIRA_URL")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")

# Optional limits for tool output (0 = no limit, full dataset returned)
PENDO_MAX_RESULTS = int(os.environ.get("PENDO_MAX_RESULTS", "0"))
PENDO_MAX_OUTPUT_CHARS = int(os.environ.get("PENDO_MAX_OUTPUT_CHARS", "0"))
