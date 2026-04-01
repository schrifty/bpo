"""Google Sheets chart creation for embedding in Slides.

Workflow:
  1. Create (or reuse) a temp spreadsheet for a deck's chart data.
  2. For each chart: add a sheet tab, populate data, create chart via addChart.
  3. Return (spreadsheet_id, chart_id) for embedding via Slides createSheetsChart.

The spreadsheet is kept in the same Drive folder as the deck so CSMs can
inspect or tweak the underlying data.
"""

from __future__ import annotations

from typing import Any

from googleapiclient.errors import HttpError

from .config import GOOGLE_DRIVE_FOLDER_ID, logger
from .slides_client import (
    _get_service, NAVY, BLUE, TEAL, LTBLUE, MINT, GRAY, WHITE,
    MARGIN, BODY_Y, BODY_BOTTOM, CONTENT_W,
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


def _rgb_to_sheets(c: dict) -> dict:
    """Convert our {red, green, blue} floats to Sheets colorStyle format."""
    return {"rgbColor": {"red": c["red"], "green": c["green"], "blue": c["blue"]}}


def _embedded_chart_border(c: dict = NAVY) -> dict:
    """Build a visible border for embedded Sheets charts."""
    return {
        "colorStyle": _rgb_to_sheets(c),
    }


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

    def _create_chart(self, sheet_id: int, spec: dict, num_rows: int) -> int:
        """Create a chart in the spreadsheet and return its chartId."""
        ss_id = self._ensure_spreadsheet()
        resp = self._sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=ss_id,
            body={"requests": [{"addChart": {"chart": {
                "spec": spec,
                "border": _embedded_chart_border(),
                "position": {"overlayPosition": {
                    "anchorCell": {"sheetId": sheet_id, "rowIndex": num_rows + 2, "columnIndex": 0},
                    "widthPixels": 800,
                    "heightPixels": 400,
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
        axis_font_size: int = 10,
        suppress_legend: bool = False,
    ) -> tuple[str, int]:
        """Create a bar/column chart. Returns (spreadsheet_id, chart_id).

        When *suppress_legend* is True the Sheets-rendered legend is hidden;
        callers should render a slide-level legend via ``_slide_chart_legend``
        in ``slides_client`` so text is readable at presentation scale.
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
            if ci < len(BRAND_SERIES_COLORS):
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
            "titleTextFormat": _chart_text_format(12, NAVY, bold=True),
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
        axis_font_size: int = 10,
        line_width: int = 3,
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
            "titleTextFormat": _chart_text_format(12, NAVY, bold=True),
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
    ) -> tuple[str, int]:
        """Create a pie (or donut) chart. Returns (spreadsheet_id, chart_id).

        When *suppress_legend* is True (the default) the Sheets-rendered legend
        is hidden; callers should render a slide-level legend via
        ``_slide_chart_legend`` in ``slides_client`` so the text is readable
        at presentation scale.
        """
        sheet_id = self._add_sheet_tab(title)
        headers = ["Label", "Value"]
        rows = [[labels[i], values[i]] for i in range(len(labels))]
        self._populate(sheet_id, headers, rows)
        num_rows = len(rows) + 1

        pie_spec: dict[str, Any] = {
            "legendPosition": "NO_LEGEND" if suppress_legend else "RIGHT_LEGEND",
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

        spec = {
            "title": title,
            "titleTextFormat": _chart_text_format(12, NAVY, bold=True),
            "pieChart": pie_spec,
        }

        chart_id = self._create_chart(sheet_id, spec, num_rows)
        logger.debug("Created PIE chart '%s' (sheet=%d, chart=%d)", title, sheet_id, chart_id)
        return self._ss_id, chart_id

    def add_combo_chart(
        self,
        title: str,
        labels: list[str],
        bar_series: dict[str, list[float | int]],
        line_series: dict[str, list[float | int]],
    ) -> tuple[str, int]:
        """Create a combo chart (bars + lines). Returns (spreadsheet_id, chart_id)."""
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

        spec = {
            "title": title,
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
    from .slides_client import _get_service
    from googleapiclient.discovery import build

    slides_svc, _d2, _s2 = _get_service()
    creds = slides_svc._http.credentials  # reuse the already-authorized creds
    return build("sheets", "v4", credentials=creds)


def _get_chart_folder() -> str | None:
    """Return the Drive folder ID for chart data spreadsheets."""
    if not GOOGLE_DRIVE_FOLDER_ID:
        return None

    from .slides_client import _get_service
    _x, drive, _sh = _get_service()

    q = (
        f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents "
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
        "parents": [GOOGLE_DRIVE_FOLDER_ID],
    }
    folder = drive.files().create(body=meta, fields="id").execute()
    logger.info("Created chart-data folder: %s", folder["id"])
    return folder["id"]
