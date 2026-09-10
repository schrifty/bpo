"""Download/upload the KPI SQLite file to S3.

Object URI: ``CORTEX_KPI_STORE_S3_URI`` (``s3://bucket/key``). Missing object on
download creates an empty schema at the destination (first run). Other S3 errors
fail loud.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from .kpi_store import connect

DEFAULT_S3_KEY = "kpi/observations.sqlite"


class KPIStoreS3Error(RuntimeError):
    """S3 KPI store configuration or transfer failed."""


def kpi_store_s3_uri() -> str | None:
    raw = (os.environ.get("CORTEX_KPI_STORE_S3_URI") or "").strip()
    return raw or None


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Return ``(bucket, key)`` from ``s3://bucket/key``."""
    text = (uri or "").strip()
    if not text.lower().startswith("s3://"):
        raise KPIStoreS3Error(f"KPI store URI must start with s3://, got {uri!r}")
    rest = text[5:]
    bucket, sep, key = rest.partition("/")
    if not bucket or not sep or not key.strip():
        raise KPIStoreS3Error(f"KPI store URI must be s3://bucket/key, got {uri!r}")
    return bucket, key.strip()


def _not_found(exc: BaseException) -> bool:
    if not isinstance(exc, ClientError):
        return False
    code = str((exc.response or {}).get("Error", {}).get("Code") or "")
    return code in {"404", "NoSuchKey", "NotFound"}


def download_kpi_store(
    dest: str | Path,
    *,
    s3_client: Any,
    uri: str | None = None,
) -> str:
    """Copy the SQLite object to *dest*.

    Returns ``downloaded`` or ``created`` (empty schema when the object is missing).
    """
    target = Path(dest)
    resolved = (uri or kpi_store_s3_uri() or "").strip()
    if not resolved:
        raise KPIStoreS3Error(
            "CORTEX_KPI_STORE_S3_URI is unset (expected s3://bucket/kpi/observations.sqlite)"
        )
    bucket, key = parse_s3_uri(resolved)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        s3_client.download_file(bucket, key, str(target))
        return "downloaded"
    except ClientError as exc:
        if _not_found(exc):
            if target.exists():
                target.unlink()
            conn = connect(target)
            conn.close()
            return "created"
        raise KPIStoreS3Error(f"S3 download failed for s3://{bucket}/{key}: {exc}") from exc
    except (BotoCoreError, OSError) as exc:
        raise KPIStoreS3Error(f"S3 download failed for s3://{bucket}/{key}: {exc}") from exc


def upload_kpi_store(
    src: str | Path,
    *,
    s3_client: Any,
    uri: str | None = None,
) -> None:
    """Upload the SQLite file. Fails loud if the file is missing or S3 errors."""
    path = Path(src)
    if not path.is_file():
        raise KPIStoreS3Error(f"KPI store file does not exist: {path}")
    resolved = (uri or kpi_store_s3_uri() or "").strip()
    if not resolved:
        raise KPIStoreS3Error(
            "CORTEX_KPI_STORE_S3_URI is unset (expected s3://bucket/kpi/observations.sqlite)"
        )
    bucket, key = parse_s3_uri(resolved)
    try:
        s3_client.upload_file(str(path), bucket, key)
    except (ClientError, BotoCoreError, OSError) as exc:
        raise KPIStoreS3Error(f"S3 upload failed for s3://{bucket}/{key}: {exc}") from exc
