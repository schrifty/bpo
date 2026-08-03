"""Send plain-text email via Amazon SES (fail loud)."""

from __future__ import annotations

import os
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError


class SesEmailError(Exception):
    """SES send failed or configuration is incomplete."""


def digest_email_recipients() -> list[str]:
    """Parse ``CORTEX_METRICS_DIGEST_TO`` (comma-separated) into non-empty addresses."""
    raw = (os.environ.get("CORTEX_METRICS_DIGEST_TO") or "").strip()
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def digest_email_from() -> str:
    """Return ``CORTEX_METRICS_DIGEST_FROM`` or empty string."""
    return (os.environ.get("CORTEX_METRICS_DIGEST_FROM") or "").strip()


def send_email(
    *,
    to: list[str] | str,
    subject: str,
    body: str,
    from_addr: str | None = None,
    region: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    """Send a plain-text email via SES ``SendEmail``.

    Raises :class:`SesEmailError` when From/To are missing or SES rejects the send.
    """
    recipients = [to] if isinstance(to, str) else list(to)
    recipients = [r.strip() for r in recipients if r and str(r).strip()]
    source = (from_addr if from_addr is not None else digest_email_from()).strip()
    if not source:
        raise SesEmailError(
            "Missing SES From address — set CORTEX_METRICS_DIGEST_FROM "
            "(verified identity in SES)."
        )
    if not recipients:
        raise SesEmailError(
            "Missing SES To address — set CORTEX_METRICS_DIGEST_TO "
            "(comma-separated recipients)."
        )
    subj = (subject or "").strip()
    if not subj:
        raise SesEmailError("Email subject must be non-empty")
    text = body if body is not None else ""

    ses = client
    if ses is None:
        ses_region = (region or os.environ.get("CORTEX_AWS_REGION") or os.environ.get("AWS_REGION") or "us-east-1").strip()
        ses = boto3.client("ses", region_name=ses_region)

    try:
        response = ses.send_email(
            Source=source,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subj, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": text, "Charset": "UTF-8"}},
            },
        )
    except (ClientError, BotoCoreError) as e:
        raise SesEmailError(f"SES SendEmail failed: {e}") from e

    message_id = ""
    if isinstance(response, dict):
        message_id = str(response.get("MessageId") or "")
    return {
        "ok": True,
        "message_id": message_id,
        "to": recipients,
        "from": source,
        "subject": subj,
    }
