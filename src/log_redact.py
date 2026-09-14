"""Redact credentials from job logs and failure artifacts.

Applied to child stdout forwarded to CloudWatch and to stdout/stderr tails
uploaded with failures.json. Customer names and HTTP status text are kept so
retry classification still works.
"""

from __future__ import annotations

import re

REDACTED = "<redacted>"

_BEARER = re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._\-+=/]{8,}")
_AUTH_HEADER = re.compile(r"(?i)\bauthorization\s*[:=]\s*(?:bearer\s+)?\S+")
_COOKIE_HEADER = re.compile(r"(?i)\b(?:set-)?cookie\s*[:=]\s*.+")
_AWS_ACCESS_KEY = re.compile(r"\bAKIA[0-9A-Z]{16}\b")
_SLACK_TOKEN = re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")
_OPENAI_KEY = re.compile(r"\bsk-[A-Za-z0-9]{16,}\b")
_ANTHROPIC_KEY = re.compile(r"\bsk-ant-[A-Za-z0-9\-_]{16,}\b")
_GOOGLE_API_KEY = re.compile(r"\bAIza[0-9A-Za-z\-_]{20,}\b")
_JWT = re.compile(r"\beyJ[A-Za-z0-9_\-]+=*\.eyJ[A-Za-z0-9_\-]+=*\.[A-Za-z0-9_\-+=/]+\b")
_QUERY_SECRET = re.compile(
    r"(?i)([?&](?:access_token|api[_-]?key|token|secret|password|key)=)[^&\s\"']+"
)
_ASSIGNED_SECRET = re.compile(
    r"(?i)\b("
    r"api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|"
    r"client[_-]?secret|private[_-]?key|secret|password|passwd"
    r")\s*([=:])\s*([\"']?)([^\s\"']{6,})(\3)"
)
_PEM = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    re.DOTALL,
)
_ENV_SECRET = re.compile(
    r"(?i)\b("
    r"(?:PR_|ST_)?LEANDNA_DATA_API_(?:API_KEY|BEARER_TOKEN|COOKIE)|"
    r"PENDO_INTEGRATION_KEY|JIRA_API_TOKEN|SF_PRIVATE_KEY|SF_CONSUMER_SECRET|"
    r"OPENAI_API_KEY|ANTHROPIC_API_KEY|GEMINI_API_KEY|SLACK_BOT_TOKEN|"
    r"GOOGLE_SERVICE_ACCOUNT_JSON|GOOGLE_APPLICATION_CREDENTIALS"
    r")\s*=\s*[^\s\"']+"
)


def redact_secrets(text: str | None) -> str:
    """Return *text* with credentials replaced by ``<redacted>``."""
    if not text:
        return text or ""
    out = text
    out = _PEM.sub(f"-----BEGIN PRIVATE KEY-----\\n{REDACTED}\\n-----END PRIVATE KEY-----", out)
    out = _AUTH_HEADER.sub(f"Authorization: {REDACTED}", out)
    out = _COOKIE_HEADER.sub(f"Cookie: {REDACTED}", out)
    out = _BEARER.sub(rf"\1{REDACTED}", out)
    out = _ENV_SECRET.sub(lambda m: f"{m.group(1)}={REDACTED}", out)
    out = _ASSIGNED_SECRET.sub(
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}{REDACTED}{m.group(5)}",
        out,
    )
    out = _QUERY_SECRET.sub(rf"\1{REDACTED}", out)
    out = _JWT.sub(REDACTED, out)
    out = _AWS_ACCESS_KEY.sub(REDACTED, out)
    out = _SLACK_TOKEN.sub(REDACTED, out)
    out = _OPENAI_KEY.sub(REDACTED, out)
    out = _ANTHROPIC_KEY.sub(REDACTED, out)
    out = _GOOGLE_API_KEY.sub(REDACTED, out)
    return out
