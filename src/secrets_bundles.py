"""Split Cortex Secrets Manager JSON into rotation-friendly bundles.

Phase 1 still loads every bundle into the process (see ``CORTEX_SECRETS_ARNS``).
Scheduled ECS jobs load only the bundles for their secret profile (see
``JOB_SECRET_PROFILE``). The default task definition still uses the full set
plus the legacy combined secret.
"""

from __future__ import annotations

from typing import Any, Mapping

BUNDLE_GOOGLE = "google"
BUNDLE_INTEGRATIONS = "integrations"
BUNDLE_LLM = "llm"
BUNDLE_SLACK = "slack"

SECRET_BUNDLES: tuple[str, ...] = (
    BUNDLE_GOOGLE,
    BUNDLE_INTEGRATIONS,
    BUNDLE_LLM,
    BUNDLE_SLACK,
)

# Dropped from SM payloads — ECS task definition sets these.
DROP_FROM_SECRET: frozenset[str] = frozenset(
    {
        "CORTEX_SKIP_DOTENV",
        "CORTEX_SECRETS_ARN",
        "CORTEX_SECRETS_ARNS",
        "CORTEX_CACHE_DIR",
        "CORTEX_LOG_FORMAT",
    }
)

GOOGLE_KEYS: frozenset[str] = frozenset({"GOOGLE_SERVICE_ACCOUNT_JSON"})
SLACK_KEYS: frozenset[str] = frozenset({"SLACK_BOT_TOKEN"})
LLM_KEYS: frozenset[str] = frozenset(
    {
        "LLM_PROVIDER",
        "GEMINI_API_KEY",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_ADMIN_API_KEY",
        "CURSOR_ADMIN_API_KEY",
    }
)


def default_secret_name(bundle: str, *, prefix: str = "cortex", environment: str = "prod") -> str:
    if bundle not in SECRET_BUNDLES:
        raise ValueError(f"unknown secret bundle {bundle!r}")
    return f"{prefix}/{environment}/{bundle}"


def bundle_for_key(key: str) -> str:
    """Return the Secrets Manager bundle that should hold *key*."""
    if key in GOOGLE_KEYS:
        return BUNDLE_GOOGLE
    if key in LLM_KEYS:
        return BUNDLE_LLM
    if key in SLACK_KEYS:
        return BUNDLE_SLACK
    return BUNDLE_INTEGRATIONS


def split_secret_payload(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Partition a combined secret/``.env`` JSON object into the four bundles."""
    bundles: dict[str, dict[str, Any]] = {name: {} for name in SECRET_BUNDLES}
    for key, val in payload.items():
        if key in DROP_FROM_SECRET or val is None:
            continue
        bundles[bundle_for_key(str(key))][str(key)] = val
    return bundles


# ECS job families: which SM bundles the task role may GetSecretValue.
# Keep in sync with infra/terraform/locals.tf job_secret_profile.
PROFILE_LLM = "llm"
PROFILE_DECKS = "decks"
PROFILE_METRICS = "metrics"
PROFILE_FULL = "full"

PROFILE_BUNDLES: dict[str, tuple[str, ...]] = {
    PROFILE_FULL: SECRET_BUNDLES,
    PROFILE_LLM: (BUNDLE_GOOGLE, BUNDLE_INTEGRATIONS, BUNDLE_LLM, BUNDLE_SLACK),
    PROFILE_DECKS: (BUNDLE_GOOGLE, BUNDLE_INTEGRATIONS),
    PROFILE_METRICS: (BUNDLE_INTEGRATIONS,),
}

JOB_SECRET_PROFILE: dict[str, str] = {
    "llm-context-portfolio-daily": PROFILE_LLM,
    "engineering-portfolio": PROFILE_LLM,
    "engineering-kpis": PROFILE_DECKS,
    "pendo-snapshot-refresh": PROFILE_DECKS,
    "pendo-ford-7d": PROFILE_DECKS,
    "pendo-ford-30d": PROFILE_DECKS,
    "pendo-top-arr-detailed": PROFILE_DECKS,
    "csr-dump-0000": PROFILE_DECKS,
    "csr-dump-0600": PROFILE_DECKS,
    "csr-dump-1200": PROFILE_DECKS,
    "csr-dump-1800": PROFILE_DECKS,
    "morning-report": PROFILE_METRICS,
}


def secret_profile_for_job(job_key: str) -> str:
    """Return ``llm``, ``decks``, ``metrics``, or ``full`` for a scheduled job key."""
    return JOB_SECRET_PROFILE.get((job_key or "").strip(), PROFILE_FULL)
