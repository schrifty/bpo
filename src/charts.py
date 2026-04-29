"""Google Sheets chart creation for embedding in Slides.

Workflow:
  1. Create (or reuse) a temp spreadsheet for a deck's chart data.
  2. For each chart: add a sheet tab, populate data, create chart via addChart.
  3. Return (spreadsheet_id, chart_id) for embedding via Slides createSheetsChart.

Spreadsheets live under a ``chart-data`` folder inside the resolved QBR Generator
folder (see :func:`drive_config.get_qbr_generator_folder_id_for_drive_config`).
"""

from __future__ import annotations

from typing import Any

from googleapiclient.errors import HttpError

from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID, logger
from .drive_config import get_qbr_generator_folder_id_for_drive_config
from .slides_api import _get_service
from .slides_theme import (
    NAVY,
    BLUE,
    TEAL,
    LTBLUE,
    MINT,
    GRAY,
    WHITE,
    MARGIN,
    BODY_Y,
    BODY_BOTTOM,
    CONTENT_W,
)

PT_TO_EMU = 12700

BRAND_SERIES_COLORS = [
    BLUE,      # #009aff
    TEAL,      # #38c0ce
    NAVY,      # #081c33
    LTBLUE,    # #7bc4fa
    MINT,      # #aefff6
    GRAY,      # #858585
]

LINE_SERIES_COLORS = [
    NAVY,                                            # dark navy
    {"red": 0.90, "green": 0.40, "blue": 0.00},     # strong orange
    {"red": 0.78, "green": 0.18, "blue": 0.18},     # strong red
    TEAL,
]

# Google Sheets default pie chart slice colors (Material Design palette, in order).
# Used in slide-level legends to match the Sheets-rendered chart slices.
# These match Google's default sequence so legends are correct without custom slice formatting.
PIE_SLICE_COLORS = [
    {"red": 0.86, "green": 0.27, "blue": 0.22},     # #DB4437 red
    {"red": 0.96, "green": 0.71, "blue": 0.00},     # #F4B400 yellow/gold
    {"red": 0.06, "green": 0.62, "blue": 0.35},     # #0F9D58 green
    {"red": 0.26, "green": 0.52, "blue": 0.96},     # #4285F4 blue
    {"red": 0.67, "green": 0.28, "blue": 0.74},     # #AB47BC purple
    {"red": 0.00, "green": 0.67, "blue": 0.76},     # #00ACC1 cyan
    {"red": 1.00, "green": 0.44, "blue": 0.26},     # #FF7043 deep orange
    {"red": 0.62, "green": 0.62, "blue": 0.62},     # #9E9E9E gray
]


def pie_chart_slide_legend_entries(labels: list[str]) -> list[tuple[str, dict[str, float]]]:
    """Build (label, color) rows for ``slide_chart_legend*`` next to an embedded Sheets ``pieChart``.

    Sheets does not expose per-slice colors on ``PieChartSpec``; embedded pies use Google's
    default palette in domain order. Use ``PIE_SLICE_COLORS`` — not ``BRAND_SERIES_COLORS`` —
    or the swatches will not match the donut/pie slices (see behavioral depth, engagement).
    """
    n = len(PIE_SLICE_COLORS) or 1
    return [(str(label), PIE_SLICE_COLORS[i % n]) for i, label in enumerate(labels)]


def _rgb_to_sheets(c: dict) -> dict:
    """Convert our {red, green, blue} floats to Sheets colorStyle format."""
    return {"rgbColor": {"red": c["red"], "green": c["green"], "blue": c["blue"]}}


def _embedded_chart_border(c: dict = NAVY) -> dict:
    """Build a visible border for embedded Sheets charts."""
    return {
        "colorStyle": _rgb_to_sheets(c),
    }


CHART_TITLE_PT = 36
# Min axis / category label size for embedded charts (Sliders scale charts down; 10pt became unreadable).
CHART_AXIS_PT = 12
# Applied to ChartSpec so legend text scales with chart body when the API allows.
CHART_SPEC_FONT_NAME = "Roboto"
# Pies are embedded at slide PT size; larger backing pixels + maximized spec improve the bitmap
# when Slides downscales the chart. For ticket metrics breakdown slides, the in-chart legend is
# off (NO_LEGEND) and readable copy lives in _slide_chart_legend_vertical — see
# docs/SLIDE_DESIGN_STANDARDS.md (Pie charts: Jira ticket metrics breakdown).
CHART_PIE_OVERLAY_W_PX = 2560
CHART_PIE_OVERLAY_H_PX = 1600

def _chart_text_format(font_size: int, color: dict = NAVY, bold: bool = False) -> dict:
    """Build Sheets TextFormat for chart labels/titles."""
    return {
        "fontFamily": "Roboto",
        "fontSize": font_size,
        "bold": bold,
        "foregroundColorStyle": _rgb_to_sheets(color),
    }


class DeckCharts:
    """Manages a per-deck spreadsheet for chart data.

    Usage:
        charts = DeckCharts("Bombardier — CS Health Review")
        ss_id, chart_id = charts.add_bar_chart(
            title="Feature Usage",
            labels=["Buyers", "Planners", "Alerts"],
            series={"Active": [120, 85, 40], "Total": [200, 150, 60]},
        )
        # Then in slide builder: _embed_chart(reqs, oid, sid, ss_id, chart_id, x, y, w, h)
    """

    def __init__(self, deck_name: str, folder_id: str | None = None):
        self._slides_svc, self._drive_svc, self._sheets_svc = _get_service()
        self._sheets_svc = _build_sheets_service()
        self._folder_id = folder_id or _get_chart_folder()
        self._ss_id: str | None = None
        self._sheet_counter = 0
        self._deck_name = deck_name

    @property
    def spreadsheet_id(self) -> str | None:
        return self._ss_id

    def _ensure_spreadsheet(self) -> str:
        """Create the backing spreadsheet on first use."""
        if self._ss_id:
            return self._ss_id

        import socket
        old_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(30.0)  # 30 second timeout for Sheets/Drive API
            
            body: dict[str, Any] = {
                "properties": {"title": f"{self._deck_name} — Chart Data"},
            }
            ss = self._sheets_svc.spreadsheets().create(body=body, fields="spreadsheetId").execute()
            self._ss_id = ss["spreadsheetId"]
            logger.info("Created chart spreadsheet %s", self._ss_id)

            if self._folder_id:
                try:
                    self._drive_svc.files().update(
                        fileId=self._ss_id,
                        addParents=self._folder_id,
                        fields="id,parents",
                    ).execute()
                except HttpError as e:
                    logger.warning("Could not move chart spreadsheet to folder: %s", e)
        finally:
            socket.setdefaulttimeout(old_timeout)

        return self._ss_id

    def _add_sheet_tab(self, title: str) -> int:
        """Add a new sheet tab and return its sheetId (integer)."""
        ss_id = self._ensure_spreadsheet()
        self._sheet_counter += 1
        sheet_id = self._sheet_counter
        clean_title = title[:100].replace("/", "-")

        self._sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=ss_id,
            body={"requests": [{"addSheet": {
                "properties": {"sheetId": sheet_id, "title": clean_title},
            }}]},
        ).execute()
        return sheet_id

    def _populate(self, sheet_id: int, headers: list[str], rows: list[list]) -> None:
        """Write header row + data rows into a sheet tab."""
        ss_id = self._ensure_spreadsheet()
        sheet_meta = self._sheets_svc.spreadsheets().get(
            spreadsheetId=ss_id, fields="sheets.properties"
        ).execute()
        sheet_title = None
        for s in sheet_meta.get("sheets", []):
            if s["properties"]["sheetId"] == sheet_id:
                sheet_title = s["properties"]["title"]
                break
        if not sheet_title:
            sheet_title = f"Sheet{sheet_id}"

        all_rows = [headers] + rows
        range_str = f"'{sheet_title}'!A1"

        self._sheets_svc.spreadsheets().values().update(
            spreadsheetId=ss_id,
            range=range_str,
            valueInputOption="RAW",
            body={"values": all_rows},
        ).execute()

    def _create_chart(
        self,
        sheet_id: int,
        spec: dict,
        num_rows: int,
        *,
        width_pixels: int = 800,
        height_pixels: int = 400,
    ) -> int:
        """Create a chart in the spreadsheet and return its chartId."""
        ss_id = self._ensure_spreadsheet()
        resp = self._sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=ss_id,
            body={"requests": [{"addChart": {"chart": {
                "spec": spec,
                "border": _embedded_chart_border(),
                "position": {"overlayPosition": {
                    "anchorCell": {"sheetId": sheet_id, "rowIndex": num_rows + 2, "columnIndex": 0},
                    "widthPixels": width_pixels,
                    "heightPixels": height_pixels,
                }},
            }}}]},
        ).execute()

        for reply in resp.get("replies", []):
            chart = reply.get("addChart", {}).get("chart", {})
            if "chartId" in chart:
                return chart["chartId"]

        raise RuntimeError("addChart did not return a chartId")

    # ── Public chart builders ──

    def add_bar_chart(
        self,
        title: str,
        labels: list[str],
        series: dict[str, list[float | int]],
        horizontal: bool = False,
        stacked: bool = False,
        show_title: bool = True,
        axis_font_size: int = CHART_AXIS_PT,
        suppress_legend: bool = False,
        *,
        background: dict[str, float] | None = None,
        series_colors: list[dict[str, float]] | None = None,
    ) -> tuple[str, int]:
        """Create a bar/column chart. Returns (spreadsheet_id, chart_id).

        When *suppress_legend* is True the Sheets-rendered legend is hidden;
        callers should render a slide-level legend via ``_slide_chart_legend``
        in ``slides_client`` so text is readable at presentation scale.

        Optional *series_colors* (same order as *series* keys) overrides
        ``BRAND_SERIES_COLORS`` per series — use a ``PIE_SLICE_COLORS`` prefix when a
        stacked bar sits beside an embedded pie so one legend matches both charts.
        """
        sheet_id = self._add_sheet_tab(title or "Chart")
        series_names = list(series.keys())
        headers = ["Label"] + series_names
        rows = [[labels[i]] + [s[i] for s in series.values()] for i in range(len(labels))]
        self._populate(sheet_id, headers, rows)
        num_rows = len(rows) + 1

        chart_type = "BAR" if horizontal else "COLUMN"
        chart_series = []
        for ci, name in enumerate(series_names):
            s: dict[str, Any] = {
                "series": {"sourceRange": {"sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": num_rows,
                    "startColumnIndex": ci + 1, "endColumnIndex": ci + 2,
                }]}},
                "targetAxis": "BOTTOM_AXIS" if horizontal else "LEFT_AXIS",
            }
            if series_colors is not None and ci < len(series_colors):
                s["colorStyle"] = _rgb_to_sheets(series_colors[ci])
            elif ci < len(BRAND_SERIES_COLORS):
                s["colorStyle"] = _rgb_to_sheets(BRAND_SERIES_COLORS[ci])
            chart_series.append(s)

        if suppress_legend:
            legend_pos = "NO_LEGEND"
        elif len(series_names) > 1:
            legend_pos = "BOTTOM_LEGEND"
        else:
            legend_pos = "NO_LEGEND"

        spec: dict[str, Any] = {
            "title": title if show_title else "",
            "fontName": CHART_SPEC_FONT_NAME,
            "basicChart": {
                "chartType": chart_type,
                "legendPosition": legend_pos,
                "axis": [
                    {"position": "BOTTOM_AXIS", "format": _chart_text_format(axis_font_size, GRAY)},
                    {"position": "LEFT_AXIS", "format": _chart_text_format(axis_font_size, GRAY)},
                ],
                "domains": [{"domain": {"sourceRange": {"sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": num_rows,
                    "startColumnIndex": 0, "endColumnIndex": 1,
                }]}}}],
                "series": chart_series,
                "headerCount": 1,
            },
        }
        if stacked:
            spec["basicChart"]["stackedType"] = "STACKED"

        if background is not None:
            spec["backgroundColorStyle"] = _rgb_to_sheets(background)

        if show_title and (title or "").strip():
            spec["titleTextFormat"] = _chart_text_format(CHART_TITLE_PT, NAVY, bold=True)

        chart_id = self._create_chart(sheet_id, spec, num_rows)
        logger.debug("Created %s chart '%s' (sheet=%d, chart=%d)", chart_type, title, sheet_id, chart_id)
        return self._ss_id, chart_id

    def add_line_chart(
        self,
        title: str,
        labels: list[str],
        series: dict[str, list[float | int]],
        series_colors: list[dict[str, float]] | None = None,
        show_legend: bool = True,
        axis_font_size: int = CHART_AXIS_PT,
        line_width: int = 3,
        *,
        background: dict[str, float] | None = None,
    ) -> tuple[str, int]:
        """Create a line chart. Returns (spreadsheet_id, chart_id)."""
        sheet_id = self._add_sheet_tab(title)
        series_names = list(series.keys())
        headers = ["Label"] + series_names
        rows = [[labels[i]] + [s[i] for s in series.values()] for i in range(len(labels))]
        self._populate(sheet_id, headers, rows)
        num_rows = len(rows) + 1

        chart_series = []
        for ci, name in enumerate(series_names):
            s: dict[str, Any] = {
                "series": {"sourceRange": {"sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": num_rows,
                    "startColumnIndex": ci + 1, "endColumnIndex": ci + 2,
                }]}},
                "targetAxis": "LEFT_AXIS",
                "lineStyle": {"type": "SOLID", "width": line_width},
            }
            colors = series_colors or LINE_SERIES_COLORS
            if ci < len(colors):
                s["colorStyle"] = _rgb_to_sheets(colors[ci])
            chart_series.append(s)

        spec = {
            "title": title,
            "titleTextFormat": _chart_text_format(CHART_TITLE_PT, NAVY, bold=True),
            "fontName": CHART_SPEC_FONT_NAME,
            "basicChart": {
                "chartType": "LINE",
                "legendPosition": ("BOTTOM_LEGEND" if len(series_names) > 1 else "NO_LEGEND") if show_legend else "NO_LEGEND",
                "axis": [
                    {"position": "BOTTOM_AXIS", "format": _chart_text_format(axis_font_size, GRAY)},
                    {"position": "LEFT_AXIS", "format": _chart_text_format(axis_font_size, GRAY)},
                ],
                "domains": [{"domain": {"sourceRange": {"sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": num_rows,
                    "startColumnIndex": 0, "endColumnIndex": 1,
                }]}}}],
                "series": chart_series,
                "headerCount": 1,
            },
        }

        if background is not None:
            spec["backgroundColorStyle"] = _rgb_to_sheets(background)

        chart_id = self._create_chart(sheet_id, spec, num_rows)
        logger.debug("Created LINE chart '%s' (sheet=%d, chart=%d)", title, sheet_id, chart_id)
        return self._ss_id, chart_id

    def add_pie_chart(
        self,
        title: str,
        labels: list[str],
        values: list[float | int],
        donut: bool = False,
        suppress_legend: bool = True,
        show_title: bool = True,
        legend_position: str = "BOTTOM_LEGEND",
        *,
        background: dict[str, float] | None = None,
        maximized: bool = True,
    ) -> tuple[str, int]:
        """Create a pie (or donut) chart. Returns (spreadsheet_id, chart_id).

        When *suppress_legend* is True (the default) the Sheets-rendered legend
        is hidden; callers should render a slide-level legend via
        ``_slide_chart_legend`` in ``slides_client`` so the text is readable
        at presentation scale. Swatches must use ``pie_chart_slide_legend_entries``
        (Sheets default slice colors), not ``BRAND_SERIES_COLORS``.

        *maximized* reduces padding in the chart object; larger overlay pixels
        (``CHART_PIE_OVERLAY_*_PX``) make embedded bitmaps sharper. There is
        no Sheets API to set pie legend font size directly.
        """
        sheet_id = self._add_sheet_tab(title)
        headers = ["Label", "Value"]
        rows = [[labels[i], values[i]] for i in range(len(labels))]
        self._populate(sheet_id, headers, rows)
        num_rows = len(rows) + 1

        pie_spec: dict[str, Any] = {
            "legendPosition": "NO_LEGEND" if suppress_legend else legend_position,
            "domain": {"sourceRange": {"sources": [{
                "sheetId": sheet_id,
                "startRowIndex": 0, "endRowIndex": num_rows,
                "startColumnIndex": 0, "endColumnIndex": 1,
            }]}},
            "series": {"sourceRange": {"sources": [{
                "sheetId": sheet_id,
                "startRowIndex": 0, "endRowIndex": num_rows,
                "startColumnIndex": 1, "endColumnIndex": 2,
            }]}},
        }
        if donut:
            pie_spec["pieHole"] = 0.4

        # Omit titleTextFormat when there is no title: a 36pt title spec still affects layout
        # and can shrink the pie + LABELED_LEGEND callout text in the render.
        spec: dict[str, Any] = {
            "title": title if show_title else "",
            "fontName": CHART_SPEC_FONT_NAME,
            "pieChart": pie_spec,
        }
        if show_title:
            spec["titleTextFormat"] = _chart_text_format(CHART_TITLE_PT, NAVY, bold=True)
        if maximized:
            spec["maximized"] = True

        if background is not None:
            spec["backgroundColorStyle"] = _rgb_to_sheets(background)

        chart_id = self._create_chart(
            sheet_id, spec, num_rows,
            width_pixels=CHART_PIE_OVERLAY_W_PX,
            height_pixels=CHART_PIE_OVERLAY_H_PX,
        )
        logger.debug("Created PIE chart '%s' (sheet=%d, chart=%d)", title, sheet_id, chart_id)
        return self._ss_id, chart_id

    def add_combo_chart(
        self,
        title: str,
        labels: list[str],
        bar_series: dict[str, list[float | int]],
        line_series: dict[str, list[float | int]],
        *,
        background: dict[str, float] | None = None,
        show_title: bool = True,
    ) -> tuple[str, int]:
        """Create a combo chart (bars + lines). Returns (spreadsheet_id, chart_id).

        Set *show_title* False when the slide already states the takeaway — the in-chart
        title otherwise steals vertical space from the plot on tight layouts.
        """
        sheet_id = self._add_sheet_tab(title)
        all_series_names = list(bar_series.keys()) + list(line_series.keys())
        headers = ["Label"] + all_series_names
        rows = []
        for i in range(len(labels)):
            row = [labels[i]]
            for s in bar_series.values():
                row.append(s[i])
            for s in line_series.values():
                row.append(s[i])
            rows.append(row)
        self._populate(sheet_id, headers, rows)
        num_rows = len(rows) + 1

        chart_series = []
        col = 1
        for ci, name in enumerate(bar_series):
            s: dict[str, Any] = {
                "series": {"sourceRange": {"sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": num_rows,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                }]}},
                "targetAxis": "LEFT_AXIS",
                "type": "COLUMN",
            }
            if ci < len(BRAND_SERIES_COLORS):
                s["colorStyle"] = _rgb_to_sheets(BRAND_SERIES_COLORS[ci])
            chart_series.append(s)
            col += 1

        for ci, name in enumerate(line_series):
            color_idx = len(bar_series) + ci
            s = {
                "series": {"sourceRange": {"sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": num_rows,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                }]}},
                "targetAxis": "RIGHT_AXIS",
                "type": "LINE",
            }
            if color_idx < len(BRAND_SERIES_COLORS):
                s["colorStyle"] = _rgb_to_sheets(BRAND_SERIES_COLORS[color_idx])
            chart_series.append(s)
            col += 1

        spec: dict[str, Any] = {
            "title": title if show_title else "",
            "fontName": CHART_SPEC_FONT_NAME,
            "basicChart": {
                "chartType": "COMBO",
                "legendPosition": "BOTTOM_LEGEND",
                "axis": [
                    {"position": "BOTTOM_AXIS"},
                    {"position": "LEFT_AXIS"},
                    {"position": "RIGHT_AXIS"},
                ],
                "domains": [{"domain": {"sourceRange": {"sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": num_rows,
                    "startColumnIndex": 0, "endColumnIndex": 1,
                }]}}}],
                "series": chart_series,
                "headerCount": 1,
            },
        }
        if show_title and (title or "").strip():
            spec["titleTextFormat"] = _chart_text_format(CHART_TITLE_PT, NAVY, bold=True)

        if background is not None:
            spec["backgroundColorStyle"] = _rgb_to_sheets(background)

        chart_id = self._create_chart(sheet_id, spec, num_rows)
        logger.debug("Created COMBO chart '%s' (sheet=%d, chart=%d)", title, sheet_id, chart_id)
        return self._ss_id, chart_id


# ── Slide embedding ──

def embed_chart(
    reqs: list[dict],
    oid: str,
    page_id: str,
    spreadsheet_id: str,
    chart_id: int,
    x: float, y: float, w: float, h: float,
    linked: bool = True,
) -> None:
    """Append a createSheetsChart request to embed a chart on a slide.

    Coordinates are in PT (matching slides_client conventions).
    """
    reqs.append({
        "createSheetsChart": {
            "objectId": oid,
            "spreadsheetId": spreadsheet_id,
            "chartId": chart_id,
            "linkingMode": "LINKED" if linked else "NOT_LINKED_IMAGE",
            "elementProperties": {
                "pageObjectId": page_id,
                "size": {
                    "width": {"magnitude": w * PT_TO_EMU, "unit": "EMU"},
                    "height": {"magnitude": h * PT_TO_EMU, "unit": "EMU"},
                },
                "transform": {
                    "scaleX": 1, "scaleY": 1,
                    "translateX": x * PT_TO_EMU,
                    "translateY": y * PT_TO_EMU,
                    "unit": "EMU",
                },
            },
        }
    })


# ── Service builders ──

def _build_sheets_service():
    """Build an authenticated Sheets API service reusing existing credentials.

    The drive scope (already authorized for domain-wide delegation) covers
    Sheets API access, so we reuse the same credential chain as Slides/Drive.
    """
    from .slides_api import _get_service
    from googleapiclient.discovery import build

    slides_svc, _d2, _s2 = _get_service()
    creds = slides_svc._http.credentials  # reuse the already-authorized creds
    return build("sheets", "v4", credentials=creds)


def _get_chart_folder() -> str | None:
    """Return the Drive folder ID for chart data spreadsheets (child of QBR Generator)."""
    if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
        return None
    parent = get_qbr_generator_folder_id_for_drive_config()

    from .slides_api import _get_service
    _x, drive, _sh = _get_service()

    q = (
        f"'{parent}' in parents "
        "and name = 'chart-data' "
        "and mimeType = 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )
    results = drive.files().list(q=q, fields="files(id)", pageSize=1).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    meta = {
        "name": "chart-data",
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent],
    }
    folder = drive.files().create(body=meta, fields="id").execute()
    logger.info("Created chart-data folder: %s", folder["id"])
    return folder["id"]
