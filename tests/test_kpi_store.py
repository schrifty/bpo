"""SQLite KPI store and S3 adapter (no live AWS)."""

from __future__ import annotations

from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from src.kpi_observation import KPIObservation
from src.kpi_store import (
    CUSTOMER_REPORTED_BUGS_METRIC,
    GRAIN_DAILY,
    GRAIN_MONTH,
    KPIStoreError,
    connect,
    get_kpi,
    list_kpis,
    stored_kpi_from_observation,
    upsert_kpi,
)
from src.kpi_store_s3 import (
    KPIStoreS3Error,
    download_kpi_store,
    parse_s3_uri,
    upload_kpi_store,
)


def _obs(**kwargs) -> KPIObservation:
    defaults = dict(value=12.5, numerator=25.0, denominator=2.0, as_of="2026-09-10", origin="live")
    defaults.update(kwargs)
    return KPIObservation(**defaults)


def test_parse_s3_uri() -> None:
    assert parse_s3_uri("s3://my-bucket/kpi/observations.sqlite") == (
        "my-bucket",
        "kpi/observations.sqlite",
    )
    with pytest.raises(KPIStoreS3Error, match="s3://"):
        parse_s3_uri("https://example/x")
    with pytest.raises(KPIStoreS3Error, match="bucket/key"):
        parse_s3_uri("s3://bucket-only")


def test_connect_creates_schema(tmp_path: Path) -> None:
    path = tmp_path / "kpi.sqlite"
    conn = connect(path)
    ver = conn.execute("SELECT value FROM kpi_store_meta WHERE key = 'schema_version'").fetchone()
    assert ver["value"] == "1"
    conn.close()
    assert path.is_file()


def test_upsert_get_and_replace(tmp_path: Path) -> None:
    conn = connect(tmp_path / "kpi.sqlite")
    first = stored_kpi_from_observation(
        metric_name="PRs Merged",
        grain=GRAIN_MONTH,
        period_key="2026-08",
        observation=_obs(value=100, as_of="2026-08-01"),
        generator="get_prs_merged",
        tags=["engineering", "impact"],
        captured_at="2026-09-01T12:00:00Z",
    )
    upsert_kpi(conn, first)
    got = get_kpi(conn, "PRs Merged", GRAIN_MONTH, "2026-08")
    assert got is not None
    assert got.observation.value == 100
    assert got.observation.origin == "stored"
    assert got.observation.source == ("kpi-store",)
    assert got.tags == ("engineering", "impact")
    assert got.generator == "get_prs_merged"

    second = stored_kpi_from_observation(
        metric_name="PRs Merged",
        grain=GRAIN_MONTH,
        period_key="2026-08",
        observation=_obs(value=110, as_of="2026-08-01"),
        generator="get_prs_merged",
        tags=["engineering"],
        captured_at="2026-09-02T12:00:00Z",
    )
    upsert_kpi(conn, second)
    rows = list_kpis(conn, metric_name="PRs Merged")
    assert len(rows) == 1
    assert rows[0].observation.value == 110
    assert rows[0].captured_at == "2026-09-02T12:00:00Z"
    conn.close()


def test_daily_and_month_grains_are_independent(tmp_path: Path) -> None:
    conn = connect(tmp_path / "kpi.sqlite")
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="SLA Adherence (30 Days)",
            grain=GRAIN_DAILY,
            period_key="2026-09-10",
            observation=_obs(value=18),
            generator="get_sla_adherence",
        ),
    )
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Issues Shipped",
            grain=GRAIN_MONTH,
            period_key="2026-08",
            observation=_obs(value=280),
            generator="get_issues_shipped",
        ),
    )
    daily = list_kpis(conn, grain=GRAIN_DAILY)
    month = list_kpis(conn, grain=GRAIN_MONTH)
    assert [r.metric_name for r in daily] == ["SLA Adherence (30 Days)"]
    assert [r.metric_name for r in month] == ["Issues Shipped"]
    conn.close()


def test_retired_daily_open_bug_rows_deleted_on_connect(tmp_path: Path) -> None:
    db = tmp_path / "kpi.sqlite"
    conn = connect(db)
    conn.execute(
        """
        INSERT INTO kpi_observation (
            metric_name, grain, period_key, captured_at,
            value, numerator, denominator, generator,
            tags_json, meta_json, error, as_of, window_days
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', '{}', NULL, ?, NULL)
        """,
        (
            CUSTOMER_REPORTED_BUGS_METRIC,
            GRAIN_DAILY,
            "2026-08-31",
            "2026-09-01T00:00:00Z",
            22.0,
            22.0,
            1.0,
            "get_customer_reported_bugs",
            "2026-08-31",
        ),
    )
    conn.commit()
    conn.close()
    conn = connect(db)
    assert get_kpi(conn, CUSTOMER_REPORTED_BUGS_METRIC, GRAIN_DAILY, "2026-08-31") is None
    month = stored_kpi_from_observation(
        metric_name=CUSTOMER_REPORTED_BUGS_METRIC,
        grain=GRAIN_MONTH,
        period_key="2026-08",
        observation=_obs(value=48),
        generator="get_customer_reported_bugs_created",
    )
    upsert_kpi(conn, month)
    got = get_kpi(conn, CUSTOMER_REPORTED_BUGS_METRIC, GRAIN_MONTH, "2026-08")
    assert got is not None
    assert got.observation.value == 48
    conn.close()


def test_retired_combined_escalation_rate_deleted_on_connect(tmp_path: Path) -> None:
    db = tmp_path / "kpi.sqlite"
    conn = connect(db)
    conn.execute(
        """
        INSERT INTO kpi_observation (
            metric_name, grain, period_key, captured_at,
            value, numerator, denominator, generator,
            tags_json, meta_json, error, as_of, window_days
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', '{}', NULL, ?, NULL)
        """,
        (
            "Escalation Rate (30 Days)",
            GRAIN_DAILY,
            "2026-09-10",
            "2026-09-10T00:00:00Z",
            87.5,
            14.0,
            16.0,
            "get_help_escalation_rate",
            "2026-09-10",
        ),
    )
    conn.commit()
    conn.close()
    conn = connect(db)
    assert get_kpi(conn, "Escalation Rate (30 Days)", GRAIN_DAILY, "2026-09-10") is None
    conn.close()


def test_error_row_is_persisted(tmp_path: Path) -> None:
    conn = connect(tmp_path / "kpi.sqlite")
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="AI Code Share",
            grain=GRAIN_MONTH,
            period_key="2026-08",
            observation=KPIObservation(error="Cursor unavailable", origin="live"),
            generator="get_ai_code_share",
        ),
    )
    got = get_kpi(conn, "AI Code Share", GRAIN_MONTH, "2026-08")
    assert got is not None
    assert got.observation.ok is False
    assert got.observation.error == "Cursor unavailable"
    conn.close()


def test_invalid_grain_and_period_key() -> None:
    with pytest.raises(KPIStoreError, match="daily.*month"):
        stored_kpi_from_observation(
            metric_name="X",
            grain="weekly",
            period_key="2026-09-10",
            observation=_obs(),
        )
    with pytest.raises(KPIStoreError, match="YYYY-MM-DD"):
        stored_kpi_from_observation(
            metric_name="X",
            grain=GRAIN_DAILY,
            period_key="2026-09",
            observation=_obs(),
        )
    with pytest.raises(KPIStoreError, match="invalid period_key"):
        stored_kpi_from_observation(
            metric_name="X",
            grain=GRAIN_DAILY,
            period_key="2026-13-40",
            observation=_obs(),
        )


class _MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        blob = self.objects.get((bucket, key))
        if blob is None:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject")
        Path(filename).write_bytes(blob)

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.objects[(bucket, key)] = Path(filename).read_bytes()


def test_s3_download_creates_empty_store_when_missing(tmp_path: Path) -> None:
    dest = tmp_path / "pulled.sqlite"
    status = download_kpi_store(
        dest,
        s3_client=_MemoryS3(),
        uri="s3://cortex-kpi/kpi/observations.sqlite",
    )
    assert status == "created"
    conn = connect(dest)
    assert list_kpis(conn) == []
    conn.close()


def test_s3_roundtrip_preserves_rows(tmp_path: Path) -> None:
    local = tmp_path / "kpi.sqlite"
    conn = connect(local)
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Issues Shipped",
            grain=GRAIN_MONTH,
            period_key="2026-08",
            observation=_obs(value=280),
            generator="get_issues_shipped",
            captured_at="2026-09-01T00:00:00Z",
        ),
    )
    conn.close()

    s3 = _MemoryS3()
    uri = "s3://cortex-kpi/kpi/observations.sqlite"
    upload_kpi_store(local, s3_client=s3, uri=uri)
    dest = tmp_path / "from-s3.sqlite"
    assert download_kpi_store(dest, s3_client=s3, uri=uri) == "downloaded"
    conn = connect(dest)
    rows = list_kpis(conn, metric_name="Issues Shipped")
    assert len(rows) == 1
    assert rows[0].observation.value == 280
    conn.close()


def test_prepare_kpi_store_for_read_skip_s3_and_local(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.kpi_store_s3 import KPIStoreS3Error, prepare_kpi_store_for_read

    missing = tmp_path / "missing.sqlite"
    with pytest.raises(KPIStoreS3Error, match="does not exist"):
        prepare_kpi_store_for_read(missing, skip_s3=True)

    local = tmp_path / "kpi.sqlite"
    connect(local).close()
    assert prepare_kpi_store_for_read(local, skip_s3=True) == local
    monkeypatch.delenv("CORTEX_KPI_STORE_S3_URI", raising=False)
    assert prepare_kpi_store_for_read(local) == local
    with pytest.raises(KPIStoreS3Error, match="CORTEX_KPI_STORE_S3_URI"):
        prepare_kpi_store_for_read(tmp_path / "nope.sqlite")


def test_prepare_kpi_store_for_read_downloads_s3(tmp_path: Path) -> None:
    from src.kpi_store_s3 import prepare_kpi_store_for_read

    src = tmp_path / "src.sqlite"
    conn = connect(src)
    upsert_kpi(
        conn,
        stored_kpi_from_observation(
            metric_name="Issues Shipped",
            grain=GRAIN_MONTH,
            period_key="2026-08",
            observation=_obs(value=280),
            generator="get_issues_shipped",
        ),
    )
    conn.close()
    s3 = _MemoryS3()
    uri = "s3://cortex-kpi/kpi/observations.sqlite"
    upload_kpi_store(src, s3_client=s3, uri=uri)
    dest = tmp_path / "from-s3.sqlite"
    got = prepare_kpi_store_for_read(dest, s3_client=s3, uri=uri)
    assert got == dest
    conn = connect(dest)
    assert list_kpis(conn, metric_name="Issues Shipped")[0].observation.value == 280
    conn.close()


def test_s3_requires_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CORTEX_KPI_STORE_S3_URI", raising=False)
    with pytest.raises(KPIStoreS3Error, match="CORTEX_KPI_STORE_S3_URI"):
        download_kpi_store(tmp_path / "x.sqlite", s3_client=_MemoryS3())
    with pytest.raises(KPIStoreS3Error, match="does not exist"):
        upload_kpi_store(tmp_path / "missing.sqlite", s3_client=_MemoryS3(), uri="s3://b/k.sqlite")
