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
