"""Pure extraction helpers for hydrate slide analysis."""

from __future__ import annotations

from typing import Any


def extract_text(element: dict[str, Any]) -> list[str]:
    """Recursively extract visible text runs from a Google Slides page element."""
    texts: list[str] = []

    shape_text = element.get("shape", {}).get("text", {})
    for text_element in shape_text.get("textElements", []):
        content = text_element.get("textRun", {}).get("content", "").strip()
        if content:
            texts.append(content)

    table = element.get("table", {})
    for row in table.get("tableRows", []):
        for cell in row.get("tableCells", []):
            for text_element in cell.get("text", {}).get("textElements", []):
                content = text_element.get("textRun", {}).get("content", "").strip()
                if content:
                    texts.append(content)

    group = element.get("elementGroup", {})
    for child in group.get("children", []):
        texts.extend(extract_text(child))

    return texts


def describe_elements(slide: dict[str, Any]) -> dict[str, int]:
    """Summarize visual element types on a Google Slides page."""
    counts = {"text_boxes": 0, "tables": 0, "images": 0, "shapes": 0, "charts": 0}
    for element in slide.get("pageElements", []):
        if "table" in element:
            counts["tables"] += 1
        elif "image" in element:
            counts["images"] += 1
        elif "sheetsChart" in element:
            counts["charts"] += 1
        elif "shape" in element:
            if element["shape"].get("text", {}).get("textElements"):
                counts["text_boxes"] += 1
            else:
                counts["shapes"] += 1
        elif "elementGroup" in element:
            counts["shapes"] += 1
    return counts


def extract_slide_text_elements(page_elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract text and visual-data markers from slide elements."""
    items: list[dict[str, Any]] = []
    for element in page_elements:
        object_id = element.get("objectId", "")

        if element.get("image"):
            items.append({"type": "image", "element_id": object_id, "text": "(embedded image)"})
            continue

        if element.get("sheetsChart"):
            items.append({
                "type": "chart",
                "element_id": object_id,
                "text": "(embedded chart — contains data that cannot be auto-updated)",
            })
            continue

        group = element.get("elementGroup", {})
        if group:
            items.extend(extract_slide_text_elements(group.get("children", [])))
            continue

        shape = element.get("shape", {})
        shape_props = shape.get("shapeProperties", {})
        if shape_props.get("shapeBackgroundFill", {}).get("propertyState") == "RENDERED":
            bg_fill = shape_props.get("shapeBackgroundFill", {})
            if bg_fill.get("stretchedPictureFill"):
                items.append({"type": "image", "element_id": object_id, "text": "(image in shape)"})

        text_body = shape.get("text", {})
        full_text = ""
        for text_element in text_body.get("textElements", []):
            full_text += text_element.get("textRun", {}).get("content", "")
        full_text = full_text.strip()
        if full_text:
            items.append({"type": "shape", "element_id": object_id, "text": full_text})

        table = element.get("table", {})
        if table:
            for row_index, row in enumerate(table.get("tableRows", [])):
                for col_index, cell in enumerate(row.get("tableCells", [])):
                    cell_text = ""
                    for text_element in cell.get("text", {}).get("textElements", []):
                        cell_text += text_element.get("textRun", {}).get("content", "")
                    cell_text = cell_text.strip()
                    if cell_text:
                        items.append({
                            "type": "table_cell",
                            "element_id": object_id,
                            "row": row_index,
                            "col": col_index,
                            "text": cell_text,
                        })
    return items
