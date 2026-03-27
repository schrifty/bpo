"""Tests for QBR field scan (infer types, normalize keys)."""
import sqlite3

from src import qbr_field_scan as q


def test_clear_scan_db(tmp_path):
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    q._init_db(conn)
    conn.execute("INSERT INTO qbr_data_fields (field_name, field_type) VALUES ('x', 'integer')")
    conn.commit()
    conn.close()
    assert q.clear_scan_db(db) == 1
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM qbr_data_fields").fetchone()[0] == 0
    conn.close()


def test_normalize_field_name():
    assert q.normalize_field_name(" Total Sites ") == "total_sites"
    assert q.normalize_field_name("nps-score") == "nps_score"


def test_infer_field_type_canonical():
    assert q.infer_field_type("total_sites") == "integer"
    assert q.infer_field_type("support") == "json_object"
    assert q.infer_field_type("_embedded_chart") == "embed_chart"
    assert q.infer_field_type("_embedded_image") == "embed_image"


def test_infer_field_type_heuristic():
    assert q.infer_field_type("revenue_q4") == "currency"
    assert q.infer_field_type("weekly_active_pct") == "percent"


def test_parse_presentation_id():
    assert q.parse_presentation_id("abcXYZ_012") == "abcXYZ_012"
    url = "https://docs.google.com/presentation/d/1abc123/edit"
    assert q.parse_presentation_id(url) == "1abc123"


def test_db_path_from_argv():
    assert q.db_path_from_argv(["decks", "--list-fields"]) == q.DEFAULT_SCAN_DB
    assert q.db_path_from_argv(["decks", "--list-fields", "--db", "/tmp/x.db"]) == "/tmp/x.db"
