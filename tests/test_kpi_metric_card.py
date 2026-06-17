"""Geometry tests for shared KPI metric cards."""

from __future__ import annotations

from src.slide_primitives import kpi_metric_card
from src.slides_theme import KPI_METRIC_PAD_H, KPI_METRIC_PAD_V


def _shape(reqs: list, oid: str) -> dict:
    return next(
        r["createShape"]
        for r in reqs
        if isinstance(r, dict) and r.get("createShape", {}).get("objectId") == oid
    )


def _alignment(reqs: list, oid: str) -> str:
    return next(
        r["updateShapeProperties"]["shapeProperties"]["contentAlignment"]
        for r in reqs
        if isinstance(r, dict)
        and r.get("updateShapeProperties", {}).get("objectId") == oid
    )


def test_kpi_metric_card_symmetric_vertical_padding() -> None:
    reqs: list = []
    x, y, w, h = 40.0, 100.0, 120.0, 54.0
    kpi_metric_card(reqs, "k0", "sid", x, y, w, h, "Commits (30d)", "364")

    label = _shape(reqs, "k0_l")
    value = _shape(reqs, "k0_v")
    label_y = label["elementProperties"]["transform"]["translateY"]
    value_y = value["elementProperties"]["transform"]["translateY"]
    value_h = value["elementProperties"]["size"]["height"]["magnitude"]

    assert label_y == y + KPI_METRIC_PAD_V
    assert _alignment(reqs, "k0_l") == "TOP"
    assert _alignment(reqs, "k0_v") == "BOTTOM"
    assert label_y - y == KPI_METRIC_PAD_V
    assert (y + h) - (value_y + value_h) == KPI_METRIC_PAD_V
    assert label["elementProperties"]["transform"]["translateX"] == x + KPI_METRIC_PAD_H
