"""Small Google Slides ``batchUpdate`` request builders.

These helpers only append request dictionaries; they do not call Google APIs.
"""

from __future__ import annotations

from typing import Any


def size_pt(width: float, height: float) -> dict[str, dict[str, float | str]]:
    return {
        "width": {"magnitude": width, "unit": "PT"},
        "height": {"magnitude": height, "unit": "PT"},
    }


def transform_pt(x: float, y: float) -> dict[str, float | str]:
    return {"scaleX": 1, "scaleY": 1, "translateX": x, "translateY": y, "unit": "PT"}


def append_slide(reqs: list[dict[str, Any]], slide_id: str, insertion_index: int) -> None:
    reqs.append({"createSlide": {"objectId": slide_id, "insertionIndex": insertion_index}})


def append_text_box(
    reqs: list[dict[str, Any]],
    object_id: str,
    slide_id: str,
    x: float,
    y: float,
    width: float,
    height: float,
    text: str,
) -> None:
    reqs.append({
        "createShape": {
            "objectId": object_id,
            "shapeType": "TEXT_BOX",
            "elementProperties": {
                "pageObjectId": slide_id,
                "size": size_pt(width, height),
                "transform": transform_pt(x, y),
            },
        }
    })
    if text:
        reqs.append({"insertText": {"objectId": object_id, "text": text, "insertionIndex": 0}})


def append_wrapped_text_box(
    reqs: list[dict[str, Any]],
    object_id: str,
    slide_id: str,
    x: float,
    y: float,
    width: float,
    height: float,
    text: str,
) -> None:
    """Text box that clips content to its bounding box instead of overflowing."""
    reqs.append({
        "createShape": {
            "objectId": object_id,
            "shapeType": "TEXT_BOX",
            "elementProperties": {
                "pageObjectId": slide_id,
                "size": size_pt(width, height),
                "transform": transform_pt(x, y),
            },
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": object_id,
            "shapeProperties": {"contentAlignment": "TOP"},
            "fields": "contentAlignment",
        }
    })
    if text:
        reqs.append({"insertText": {"objectId": object_id, "text": text, "insertionIndex": 0}})
