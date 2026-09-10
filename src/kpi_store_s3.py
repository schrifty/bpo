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

from .kpi_store import connect, default_kpi_store_path

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


def prepare_kpi_store_for_read(
    dest: str | Path | None = None,
    *,
    s3_client: Any | None = None,
    uri: str | None = None,
    skip_s3: bool = False,
) -> Path:
    """Return a local SQLite path ready to read.

    * ``skip_s3`` — use *dest* (or the default cache path); fail if the file is missing.
    * S3 URI set — download (missing object creates an empty schema).
    * otherwise — use an existing local file, or fail loud.
    """
    path = Path(dest) if dest is not None else default_kpi_store_path()
    if skip_s3:
        if not path.is_file():
            raise KPIStoreS3Error(f"KPI store file does not exist: {path}")
        return path
    resolved = (uri if uri is not None else kpi_store_s3_uri() or "").strip()
    if resolved:
        client = s3_client
        if client is None:
            import boto3

            client = boto3.client("s3")
        download_kpi_store(path, s3_client=client, uri=resolved)
        return path
    if path.is_file():
        return path
    raise KPIStoreS3Error(
        "CORTEX_KPI_STORE_S3_URI is unset and no local KPI store exists at "
        f"{path} (expected s3://bucket/kpi/observations.sqlite, or pass --db and --skip-s3)"
    )


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
