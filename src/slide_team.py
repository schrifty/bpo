"""Team roster slide builder."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .slide_primitives import rect as _rect, style as _style
from .slide_requests import append_slide as _slide, append_text_box as _box
from .slides_theme import BLUE, FONT, MARGIN, NAVY, SLIDE_H, SLIDE_W, WHITE


def load_teams() -> dict[str, Any]:
    """Load team rosters from teams.yaml (project root)."""
    import yaml

    path = Path(__file__).resolve().parent.parent / "teams.yaml"
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}


def team_slide(reqs: list[dict[str, Any]], sid: str, report: dict[str, Any], idx: int) -> int:
    _slide(reqs, sid, idx)

    customer = report.get("customer", "Customer")
    teams = load_teams()
    team_data = teams.get(customer, {})
    cust_members = [member.get("name", "") for member in team_data.get("customer_team", [])]
    ldna_members = [member.get("name", "") for member in team_data.get("leandna_team", [])]

    if not cust_members and not ldna_members:
        cust_members = ["(no team roster configured)"]
        ldna_members = ["(no team roster configured)"]

    panel_x = 310
    panel_w = SLIDE_W - panel_x
    _rect(reqs, f"{sid}_rpanel", sid, panel_x, 0, panel_w, SLIDE_H, BLUE)
    _rect(reqs, f"{sid}_rnav", sid, SLIDE_W - 80, 0, 80, SLIDE_H, NAVY)

    brand = "LeanDNA.com"
    _box(reqs, f"{sid}_brand", sid, panel_x + 40, SLIDE_H - 60, 200, 30, brand)
    _style(reqs, f"{sid}_brand", 0, len(brand), bold=True, size=16, color=WHITE, font=FONT)

    left_w = panel_x - MARGIN
    y = 30

    cust_hdr = f"{customer} Team"
    _box(reqs, f"{sid}_ch", sid, MARGIN, y, left_w, 24, cust_hdr)
    _style(reqs, f"{sid}_ch", 0, len(cust_hdr), bold=True, size=14, color=BLUE, font=FONT)
    y += 30

    for index, name in enumerate(cust_members[:12]):
        _box(reqs, f"{sid}_cm{index}", sid, MARGIN, y, left_w, 16, name)
        _style(reqs, f"{sid}_cm{index}", 0, len(name), bold=True, size=10, color=NAVY, font=FONT)
        y += 18

    y += 14

    ldna_hdr = "LeanDNA Team"
    _box(reqs, f"{sid}_lh", sid, MARGIN, y, left_w, 24, ldna_hdr)
    _style(reqs, f"{sid}_lh", 0, len(ldna_hdr), bold=True, size=14, color=BLUE, font=FONT)
    y += 30

    for index, name in enumerate(ldna_members[:12]):
        _box(reqs, f"{sid}_lm{index}", sid, MARGIN, y, left_w, 16, name)
        _style(reqs, f"{sid}_lm{index}", 0, len(name), bold=True, size=10, color=NAVY, font=FONT)
        y += 18

    return idx + 1
