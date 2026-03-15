"""Google Slides client for creating CS-oriented usage report decks."""

import datetime
import json
import time
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .config import GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_DRIVE_FOLDER_ID, GOOGLE_DRIVE_OWNER_EMAIL, logger

SCOPES = [
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive",
]

SLIDE_W = 720
SLIDE_H = 405

# Layout
MARGIN = 48
CONTENT_W = SLIDE_W - 2 * MARGIN
TITLE_Y = 28
BODY_Y = 80
BODY_BOTTOM = SLIDE_H - 36  # safe bottom edge (room for omission note + footer)

# ── LeanDNA APEX brand palette (from template 1o2POERqEEp…) ──
NAVY = {"red": 0.031, "green": 0.110, "blue": 0.200}    # #081c33  dark navy
BLUE = {"red": 0.0,   "green": 0.604, "blue": 1.0}      # #009aff  primary accent
LTBLUE = {"red": 0.482, "green": 0.769, "blue": 0.980}   # #7bc4fa  secondary accent
TEAL = {"red": 0.220, "green": 0.753, "blue": 0.808}     # #38c0ce  tertiary accent
MINT = {"red": 0.682, "green": 1.0,   "blue": 0.965}     # #aefff6  highlight
WHITE = {"red": 1.0,  "green": 1.0,   "blue": 1.0}
DARK = NAVY                                                # alias for readability
GRAY = {"red": 0.522, "green": 0.522, "blue": 0.522}     # #858585  secondary text
LIGHT = {"red": 0.933, "green": 0.941, "blue": 0.953}    # #eef0f3  light background
FONT = "Source Sans Pro"
FONT_SERIF = "IBM Plex Serif"
MONO = "Source Sans 3"


def _date_range(days: int, quarter_label: str | None = None,
                quarter_start: str | None = None, quarter_end: str | None = None) -> str:
    """Format a human-readable date range, with optional quarter prefix.

    If quarter_label is set (e.g. 'Q1 2026'), the output looks like
    'Q1 2026 (Jan 1 – Mar 9, 2026)'.  Otherwise plain 'Feb 7 – Mar 9, 2026'.

    When quarter_start/quarter_end are provided (ISO date strings), they are
    used for display instead of computing from days, avoiding off-by-one errors.
    """
    if quarter_start and quarter_end:
        start = datetime.date.fromisoformat(quarter_start)
        end = datetime.date.fromisoformat(quarter_end)
    else:
        end = datetime.date.today()
        start = end - datetime.timedelta(days=days)
    if start.year == end.year:
        span = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
    else:
        span = f"{start.strftime('%b %-d, %Y')} – {end.strftime('%b %-d, %Y')}"
    if quarter_label:
        return f"{quarter_label} ({span})"
    return span


def _get_service():
    """Build authenticated Slides + Drive API services."""
    creds = None
    creds_path = GOOGLE_APPLICATION_CREDENTIALS
    if creds_path:
        path = Path(creds_path)
        if path.exists():
            creds = service_account.Credentials.from_service_account_file(
                str(path), scopes=SCOPES
            )
            try:
                with open(path) as f:
                    proj_id = json.load(f).get("project_id")
                if proj_id:
                    creds = creds.with_quota_project(proj_id)
            except Exception:
                pass
            if GOOGLE_DRIVE_OWNER_EMAIL:
                owner = GOOGLE_DRIVE_OWNER_EMAIL.strip()
                if owner:
                    creds = creds.with_subject(owner)
                    logger.debug("Impersonating %s (domain-wide delegation)", owner)
            logger.debug("Using service account: %s", creds_path)
    if creds is None:
        try:
            import google.auth
            creds, _ = google.auth.default(scopes=SCOPES)
        except Exception as e:
            raise ValueError(
                "No valid credentials. Set GOOGLE_APPLICATION_CREDENTIALS or run: gcloud auth application-default login"
            ) from e
    return build("slides", "v1", credentials=creds), build("drive", "v3", credentials=creds)


# ── Primitives ──

def _sz(w, h):
    return {"width": {"magnitude": w, "unit": "PT"}, "height": {"magnitude": h, "unit": "PT"}}


def _tf(x, y):
    return {"scaleX": 1, "scaleY": 1, "translateX": x, "translateY": y, "unit": "PT"}


def _slide(reqs, sid, idx):
    reqs.append({"createSlide": {"objectId": sid, "insertionIndex": idx}})


def _bg(reqs, sid, color):
    reqs.append({
        "updatePageProperties": {
            "objectId": sid,
            "pageProperties": {"pageBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
            "fields": "pageBackgroundFill",
        }
    })


def _box(reqs, oid, sid, x, y, w, h, text):
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "TEXT_BOX",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    if text:
        reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})


def _rect(reqs, oid, sid, x, y, w, h, fill):
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": fill}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })


def _pill(reqs, oid, sid, x, y, w, h, text, bg, fg):
    reqs.append({
        "createShape": {
            "objectId": oid, "shapeType": "ROUND_RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": bg}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })
    reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})
    _style(reqs, oid, 0, len(text), bold=True, size=11, color=fg)
    _align(reqs, oid, "CENTER")


def _style(reqs, oid, start, end, bold=False, size=None, color=None, font=None, italic=False,
           link=None):
    if start >= end:
        return
    s: dict[str, Any] = {}
    f = []
    if bold:
        s["bold"] = True; f.append("bold")
    if italic:
        s["italic"] = True; f.append("italic")
    if size:
        s["fontSize"] = {"magnitude": size, "unit": "PT"}; f.append("fontSize")
    if color:
        s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
    if font:
        s["fontFamily"] = font; f.append("fontFamily")
    if link:
        s["link"] = {"url": link}; f.append("link")
    if f:
        reqs.append({
            "updateTextStyle": {
                "objectId": oid,
                "textRange": {"type": "FIXED_RANGE", "startIndex": start, "endIndex": end},
                "style": s, "fields": ",".join(f),
            }
        })


def _align(reqs, oid, alignment):
    reqs.append({
        "updateParagraphStyle": {
            "objectId": oid,
            "textRange": {"type": "ALL"},
            "style": {"alignment": alignment},
            "fields": "alignment",
        }
    })


# Red banner for "data not available" (also recorded in QA for Data Quality slide)
_BANNER_RED = {"red": 0.9, "green": 0.2, "blue": 0.2}


def _red_banner(reqs, oid, sid, x, y, w, h, text):
    """Create a red rectangle with white bold centered text (data-missing banner)."""
    reqs.append({
        "createShape": {
            "objectId": oid,
            "shapeType": "ROUND_RECTANGLE",
            "elementProperties": {"pageObjectId": sid, "size": _sz(w, h), "transform": _tf(x, y)},
        }
    })
    reqs.append({
        "updateShapeProperties": {
            "objectId": oid,
            "shapeProperties": {
                "shapeBackgroundFill": {"solidFill": {"color": {"rgbColor": _BANNER_RED}}},
                "outline": {"propertyState": "NOT_RENDERED"},
            },
            "fields": "shapeBackgroundFill,outline",
        }
    })
    reqs.append({"insertText": {"objectId": oid, "text": text, "insertionIndex": 0}})
    _style(reqs, oid, 0, len(text), bold=True, size=12, color=WHITE, font=FONT)
    _align(reqs, oid, "CENTER")


def _missing_data_slide(reqs, sid, report, idx, missing_description):
    """Render a slide with title + red banner when required data is unavailable; flag for Data Quality."""
    from .qa import qa
    entry = report.get("_current_slide") or {}
    slide_type = entry.get("slide_type", entry.get("id", "slide"))
    slide_title = entry.get("title", slide_type.replace("_", " ").title())

    report.setdefault("_missing_slide_data", []).append({
        "slide_type": slide_type,
        "slide_title": slide_title,
        "missing": missing_description,
    })
    qa.flag(
        f"Slide \"{slide_title}\": {missing_description} not available",
        severity="warning",
        internal=False,
    )

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, slide_title)
    banner_text = f"Data not available: {missing_description}"
    if len(banner_text) > 90:
        banner_text = banner_text[:87] + "..."
    _red_banner(reqs, f"{sid}_banner", sid, MARGIN, BODY_Y - 8, CONTENT_W, 28, banner_text)
    return idx + 1


def _internal_footer(reqs, sid):
    label = "INTERNAL ONLY"
    fid = f"{sid}_iof"
    _box(reqs, fid, sid, SLIDE_W - MARGIN - 80, SLIDE_H - 16, 80, 12, label)
    _style(reqs, fid, 0, len(label), size=6, color=GRAY, font=FONT)
    reqs.append({
        "updateParagraphStyle": {
            "objectId": fid,
            "textRange": {"type": "ALL"},
            "style": {"alignment": "END"},
            "fields": "alignment",
        }
    })


def _clean_table(reqs, table_id, num_rows, num_cols):
    """Strip all borders from a table, then add a thin blue header separator."""
    reqs.append({
        "updateTableBorderProperties": {
            "objectId": table_id,
            "tableRange": {
                "location": {"rowIndex": 0, "columnIndex": 0},
                "rowSpan": num_rows, "columnSpan": num_cols,
            },
            "borderPosition": "ALL",
            "tableBorderProperties": {
                "tableBorderFill": {"solidFill": {"color": {"rgbColor": WHITE}}},
                "weight": {"magnitude": 0.01, "unit": "PT"},
                "dashStyle": "SOLID",
            },
            "fields": "tableBorderFill,weight,dashStyle",
        }
    })
    reqs.append({
        "updateTableBorderProperties": {
            "objectId": table_id,
            "tableRange": {
                "location": {"rowIndex": 0, "columnIndex": 0},
                "rowSpan": 1, "columnSpan": num_cols,
            },
            "borderPosition": "BOTTOM",
            "tableBorderProperties": {
                "tableBorderFill": {"solidFill": {"color": {"rgbColor": BLUE}}},
                "weight": {"magnitude": 1, "unit": "PT"},
                "dashStyle": "SOLID",
            },
            "fields": "tableBorderFill,weight,dashStyle",
        }
    })


def _simple_table(reqs, table_id, sid, x, y, col_widths, row_h, headers, rows):
    """Create a styled table with headers and data rows.

    Returns the total height consumed so callers can position elements below.
    """
    num_rows = 1 + len(rows)
    num_cols = len(headers)
    tbl_w = sum(col_widths)
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(tbl_w, num_rows * row_h),
                "transform": _tf(x, y),
            },
            "rows": num_rows, "columns": num_cols,
        }
    })

    def _ct(row, col, text):
        if text:
            reqs.append({"insertText": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "text": str(text), "insertionIndex": 0,
            }})

    def _cs(row, col, length, **kwargs):
        if length > 0:
            reqs.append({"updateTextStyle": {
                "objectId": table_id,
                "cellLocation": {"rowIndex": row, "columnIndex": col},
                "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": length},
                "style": {k: v for k, v in {
                    "bold": kwargs.get("bold"), "fontSize": {"magnitude": kwargs.get("size", 9), "unit": "PT"},
                    "foregroundColor": {"opaqueColor": {"rgbColor": kwargs.get("color", NAVY)}} if kwargs.get("color") else None,
                    "fontFamily": kwargs.get("font", FONT),
                }.items() if v is not None},
                "fields": ",".join(f for f in ["bold", "fontSize", "foregroundColor", "fontFamily"] if kwargs.get(f.replace("fontSize", "size").replace("foregroundColor", "color").replace("fontFamily", "font"), None) is not None or f in ("fontSize", "fontFamily")),
            }})

    for ci, h in enumerate(headers):
        _ct(0, ci, h)
        _cs(0, ci, len(str(h)), bold=True, size=9, color=WHITE, font=FONT)

    for ri, row in enumerate(rows):
        for ci, val in enumerate(row):
            _ct(ri + 1, ci, str(val))
            _cs(ri + 1, ci, len(str(val)), size=9, color=NAVY, font=FONT)

    for ci, w in enumerate(col_widths):
        reqs.append({"updateTableColumnProperties": {
            "objectId": table_id, "columnIndices": [ci],
            "tableColumnProperties": {"columnWidth": {"magnitude": w, "unit": "PT"}},
            "fields": "columnWidth",
        }})

    _clean_table(reqs, table_id, num_rows, num_cols)

    for ci in range(num_cols):
        reqs.append({"updateTableCellProperties": {
            "objectId": table_id,
            "tableRange": {"location": {"rowIndex": 0, "columnIndex": ci}, "rowSpan": 1, "columnSpan": 1},
            "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": NAVY}}}},
            "fields": "tableCellBackgroundFill",
        }})

    return num_rows * row_h


def _table_cell_bg(reqs, table_id, row, col, color):
    """Set background color on a single table cell."""
    reqs.append({"updateTableCellProperties": {
        "objectId": table_id,
        "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
        "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
        "fields": "tableCellBackgroundFill",
    }})


def _omission_note(reqs, sid, omitted_names: list[str], label: str = "Not shown"):
    """Add a small italic note near the bottom listing items omitted for space."""
    if not omitted_names:
        return
    names = ", ".join(omitted_names[:8])
    if len(omitted_names) > 8:
        names += f", +{len(omitted_names) - 8} more"
    note = f"{label}: {names}"
    oid = f"{sid}_omit"
    _box(reqs, oid, sid, MARGIN, BODY_BOTTOM - 2, CONTENT_W, 14, note)
    _style(reqs, oid, 0, len(note), size=7, color=GRAY, font=FONT, italic=True)


def _slide_title(reqs, sid, text):
    """Standard content-slide title: navy text + teal underline + internal footer."""
    oid = f"{sid}_ttl"
    _box(reqs, oid, sid, MARGIN, TITLE_Y, CONTENT_W, 36, text)
    _style(reqs, oid, 0, len(text), bold=True, size=20, color=NAVY, font=FONT_SERIF)
    _rect(reqs, f"{sid}_ul", sid, MARGIN, TITLE_Y + 38, 56, 2.5, BLUE)
    _internal_footer(reqs, sid)


# ── Slide builders ──

def _title_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    acct = report["account"]
    name = report["customer"]
    sub = f"Product Usage Review  ·  {_date_range(report['days'], report.get('quarter'), report.get('quarter_start'), report.get('quarter_end'))}"
    meta = f"CSM: {acct['csm']}  |  {acct['total_sites']} sites · {acct['total_visitors']} users  |  {report['generated']}"

    _rect(reqs, f"{sid}_bar", sid, 0, 190, SLIDE_W, 3, BLUE)

    _box(reqs, f"{sid}_n", sid, MARGIN, 100, CONTENT_W, 60, name)
    _style(reqs, f"{sid}_n", 0, len(name), bold=True, size=40, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 200, CONTENT_W, 30, sub)
    _style(reqs, f"{sid}_s", 0, len(sub), size=15, color=BLUE, font=FONT)

    _box(reqs, f"{sid}_m", sid, MARGIN, 350, CONTENT_W, 24, meta)
    _style(reqs, f"{sid}_m", 0, len(meta), size=9, color=GRAY, font=FONT)

    label = "INTERNAL ONLY"
    _box(reqs, f"{sid}_int", sid, MARGIN, 160, CONTENT_W, 22, label)
    _style(reqs, f"{sid}_int", 0, len(label), bold=True, size=10, color=BLUE, font=FONT)

    return idx + 1


# ── Composite health scoring ──

_HEALTH_GOOD = {"red": 0.10, "green": 0.55, "blue": 0.28}   # green
_HEALTH_MOD  = BLUE                                            # blue
_HEALTH_BAD  = {"red": 0.78, "green": 0.18, "blue": 0.18}    # red
_HEALTH_NA   = GRAY                                            # no data

_SCORE_MAP = {"HEALTHY": 3, "MODERATE": 2, "AT RISK": 1}
_LABEL_FROM_SCORE = {3: "HEALTHY", 2: "MODERATE", 1: "AT RISK"}
_COLOR_FROM_LABEL = {"HEALTHY": _HEALTH_GOOD, "MODERATE": _HEALTH_MOD, "AT RISK": _HEALTH_BAD}


def _score_engagement(report: dict) -> tuple[str, str]:
    """Score user-engagement health from Pendo active rate. Returns (label, rationale)."""
    rate = report.get("engagement", {}).get("active_rate_7d", 0)
    if rate >= 40:
        return "HEALTHY", f"{rate}% weekly active"
    elif rate >= 20:
        return "MODERATE", f"{rate}% weekly active"
    else:
        return "AT RISK", f"{rate}% weekly active"


def _score_platform(report: dict) -> tuple[str, str] | None:
    """Score platform health from CS Report factory health scores. Returns None if no data."""
    cs = report.get("cs_platform_health", {})
    sites = cs.get("sites", [])
    if not sites:
        return None
    dist = cs.get("health_distribution", {})
    reds = dist.get("RED", 0)
    greens = dist.get("GREEN", 0)
    total = len(sites)
    pct_green = greens / max(total, 1) * 100
    if reds > 0:
        return "AT RISK", f"{reds} RED factory{'s' if reds != 1 else ''}"
    elif pct_green >= 50:
        return "HEALTHY", f"{greens}/{total} factories GREEN"
    else:
        return "MODERATE", f"{greens}/{total} factories GREEN"


def _score_support(report: dict) -> tuple[str, str] | None:
    """Score support health from Jira ticket data. Returns None if no data."""
    jira = report.get("jira", {})
    if not jira or jira.get("error") or jira.get("total_issues", 0) == 0:
        return None
    total = jira["total_issues"]
    escalated = jira.get("escalated", 0)
    open_n = jira.get("open_issues", 0)
    ttr = jira.get("ttr", {})
    breached = ttr.get("breached", 0)

    esc_pct = escalated / max(total, 1) * 100
    open_pct = open_n / max(total, 1) * 100

    if breached > 0 or esc_pct > 40:
        return "AT RISK", f"{escalated} escalated, {breached} SLA breach{'es' if breached != 1 else ''}"
    elif esc_pct > 20 or open_pct > 50:
        return "MODERATE", f"{open_n} open, {escalated} escalated"
    else:
        return "HEALTHY", f"{open_n} open, {escalated} escalated"


def _composite_health(report: dict) -> dict[str, Any]:
    """Compute composite health from all available dimensions."""
    dims: list[dict[str, Any]] = []

    eng_label, eng_why = _score_engagement(report)
    dims.append({"name": "Engagement", "label": eng_label, "detail": eng_why,
                 "source": "Pendo", "color": _COLOR_FROM_LABEL[eng_label]})

    plat = _score_platform(report)
    if plat:
        dims.append({"name": "Platform", "label": plat[0], "detail": plat[1],
                      "source": "CS Report", "color": _COLOR_FROM_LABEL[plat[0]]})

    supp = _score_support(report)
    if supp:
        dims.append({"name": "Support", "label": supp[0], "detail": supp[1],
                      "source": "Jira", "color": _COLOR_FROM_LABEL[supp[0]]})

    scores = [_SCORE_MAP[d["label"]] for d in dims]
    avg = sum(scores) / len(scores) if scores else 2
    if avg >= 2.5:
        overall = "HEALTHY"
    elif avg >= 1.5:
        overall = "MODERATE"
    else:
        overall = "AT RISK"

    return {
        "overall": overall,
        "overall_color": _COLOR_FROM_LABEL[overall],
        "dimensions": dims,
    }


def _health_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Account Health Snapshot")

    eng = report["engagement"]
    bench = report["benchmarks"]
    acct = report["account"]
    rate = eng["active_rate_7d"]
    active = eng["active_7d"] + eng["active_30d"]
    internal = acct.get("internal_visitors", 0)

    # Composite health badge
    health = _composite_health(report)
    label = health["overall"]
    badge_bg = health["overall_color"]
    _pill(reqs, f"{sid}_badge", sid, SLIDE_W - MARGIN - 110, TITLE_Y + 2, 110, 28, label, badge_bg, WHITE)

    # KPIs — use cohort benchmark when available
    cohort_name = bench.get("cohort_name", "")
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    if cohort_med is not None and cohort_n >= 3:
        vs = rate - cohort_med
        direction = "above" if vs > 0 else "below" if vs < 0 else "at"
        bench_label = f"{cohort_name} median of {cohort_med}%  ({cohort_n} peers)"
    else:
        vs = rate - bench["peer_median_rate"]
        direction = "above" if vs > 0 else "below" if vs < 0 else "at"
        bench_label = f"all-customer median of {bench['peer_median_rate']}%  ({bench['peer_count']} peers)"
    lines = [
        f"Customer Users: {acct['total_visitors']}",
        f"Active This Week: {eng['active_7d']}  ({rate}%)",
        f"Active This Month: {active}",
        f"Dormant (30+ days): {eng['dormant']}",
        "",
        f"Weekly Active Rate: {rate}%  ({abs(vs):.0f}pp {direction} {bench_label})",
        f"Sites: {acct['total_sites']}  |  Cohort: {cohort_name or 'Unclassified'}",
    ]
    if internal:
        lines.append(f"({internal} internal staff excluded)")
    kpi = "\n".join(lines)

    _box(reqs, f"{sid}_kpi", sid, MARGIN, BODY_Y, CONTENT_W // 2 + 20, 200, kpi)
    _style(reqs, f"{sid}_kpi", 0, len(kpi), size=12, color=NAVY, font=FONT)

    off = 0
    for line in lines:
        if ":" in line and line.strip() and not line.startswith("("):
            c = line.index(":")
            _style(reqs, f"{sid}_kpi", off, off + c + 1, bold=True)
        off += len(line) + 1

    # Dimension breakdown (right side)
    dims = health["dimensions"]
    dx = MARGIN + CONTENT_W // 2 + 40
    dw = CONTENT_W // 2 - 40
    dy = BODY_Y + 4

    for i, d in enumerate(dims):
        dot_map = {"HEALTHY": "\u25cf", "MODERATE": "\u25cf", "AT RISK": "\u25cf"}
        dot = dot_map.get(d["label"], "\u25cf")
        dim_line = f"{dot}  {d['name']}: {d['label']}"
        oid = f"{sid}_d{i}"
        _box(reqs, oid, sid, dx, dy, dw, 18, dim_line)
        _style(reqs, oid, 0, len(dim_line), bold=True, size=11, color=d["color"], font=FONT)

        det = f"     {d['detail']}  ({d['source']})"
        did = f"{sid}_dd{i}"
        _box(reqs, did, sid, dx, dy + 16, dw, 14, det)
        _style(reqs, did, 0, len(det), size=9, color=GRAY, font=FONT)

        dy += 44

    return idx + 1


def _engagement_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Engagement Breakdown")

    eng = report["engagement"]
    total = report["account"]["total_visitors"]

    charts = report.get("_charts")
    has_chart = False

    # Try to embed a donut chart for the tier distribution
    if charts and total > 0:
        try:
            from .charts import embed_chart
            active_7d = eng["active_7d"]
            active_30d = eng["active_30d"]
            dormant = eng["dormant"]
            ss_id, chart_id = charts.add_pie_chart(
                title="User Engagement",
                labels=["Active (7d)", "Active (8–30d)", "Dormant (30d+)"],
                values=[active_7d, active_30d, dormant],
                donut=True,
            )
            # Chart fills most of the left side of the page
            chart_w = 320
            chart_h = int(BODY_BOTTOM - BODY_Y)  # full body height
            embed_chart(reqs, f"{sid}_donut", sid, ss_id, chart_id,
                        MARGIN, BODY_Y, chart_w, chart_h)
            has_chart = True
        except Exception as e:
            logger.warning("Chart embed failed for engagement slide: %s", e)

    # Text column: right of chart when present, else full width
    chart_used_w = 344 if has_chart else 0  # chart width + gap
    text_x = MARGIN + chart_used_w if has_chart else MARGIN
    text_w = CONTENT_W - chart_used_w if has_chart else CONTENT_W
    col_gap = 40
    col_w = (text_w - col_gap) // 2 if not has_chart else text_w

    # ── Engagement tiers ──
    tiers = [
        ("Active (7d)", eng["active_7d"], BLUE),
        ("Active (8–30d)", eng["active_30d"], NAVY),
        ("Dormant (30d+)", eng["dormant"], GRAY),
    ]
    y = BODY_Y + 8
    for i, (label, count, color) in enumerate(tiers):
        pct = round(count / max(total, 1) * 100)
        num_text = f"{count}"
        _box(reqs, f"{sid}_n{i}", sid, text_x, y, 80, 36, num_text)
        _style(reqs, f"{sid}_n{i}", 0, len(num_text), bold=True, size=28, color=color, font=FONT)

        detail = f"{label}  ({pct}%)"
        _box(reqs, f"{sid}_d{i}", sid, text_x + 85, y + 8, col_w - 85, 24, detail)
        _style(reqs, f"{sid}_d{i}", 0, len(detail), size=14, color=NAVY, font=FONT)
        y += 56

    total_text = f"{total} total users"
    _box(reqs, f"{sid}_tot", sid, text_x, y + 8, col_w, 20, total_text)
    _style(reqs, f"{sid}_tot", 0, len(total_text), size=12, color=GRAY, font=FONT)

    # ── Role breakdown (below chart when chart present, right column when not) ──
    if has_chart:
        right_x = text_x
        ry = y + 40
    else:
        right_x = MARGIN + col_w + col_gap
        ry = BODY_Y + 8

    active_roles = list(eng["role_active"].items())[:6]
    dormant_roles = list(eng["role_dormant"].items())[:6]

    if active_roles:
        ah = "Active Roles"
        _box(reqs, f"{sid}_ah", sid, right_x, ry, col_w, 22, ah)
        _style(reqs, f"{sid}_ah", 0, len(ah), bold=True, size=14, color=BLUE, font=FONT)
        ry += 28
        for ri, (role, count) in enumerate(active_roles):
            if ry + 22 > BODY_BOTTOM:
                break
            line = f"{count:>4}   {role}"
            _box(reqs, f"{sid}_ar{ri}", sid, right_x, ry, col_w, 18, line)
            _style(reqs, f"{sid}_ar{ri}", 0, len(line), size=13, color=NAVY, font=FONT)
            _style(reqs, f"{sid}_ar{ri}", 0, len(f"{count:>4}"), bold=True, size=13, color=BLUE, font=FONT)
            ry += 22

    if dormant_roles and ry + 50 < BODY_BOTTOM:
        ry += 12
        dh = "Dormant Roles"
        _box(reqs, f"{sid}_dh", sid, right_x, ry, col_w, 22, dh)
        _style(reqs, f"{sid}_dh", 0, len(dh), bold=True, size=14, color=GRAY, font=FONT)
        ry += 28
        for ri, (role, count) in enumerate(dormant_roles):
            if ry + 22 > BODY_BOTTOM:
                break
            line = f"{count:>4}   {role}"
            _box(reqs, f"{sid}_dr{ri}", sid, right_x, ry, col_w, 18, line)
            _style(reqs, f"{sid}_dr{ri}", 0, len(line), size=13, color=GRAY, font=FONT)
            _style(reqs, f"{sid}_dr{ri}", 0, len(f"{count:>4}"), bold=True, size=13)
            ry += 22

    return idx + 1


def _sites_slide(reqs, sid, report, idx):
    all_sites = report["sites"]
    if not all_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo site/list data")

    customer_prefix = report.get("account", {}).get("customer", "").strip()
    has_entity = any(s.get("entity") for s in all_sites)

    def _short_site(name: str) -> str:
        n = name
        if customer_prefix and n.lower().startswith(customer_prefix.lower()):
            n = n[len(customer_prefix):].lstrip(" -·")
        return n[:18] if len(n) > 18 else n

    # Compact layout: smaller row height and fonts so we fit more rows and can paginate
    ROW_H = 18
    FONT_PT = 7
    table_top = BODY_Y

    if has_entity:
        headers = ["Site", "Entity", "Users", "Pg", "Feat", "Evt", "Min", "Last"]
        col_widths = [118, 78, 40, 44, 46, 44, 40, 58]
        end_col_start, end_col_end = 2, 6
    else:
        headers = ["Site", "Users", "Pg", "Feat", "Evt", "Min", "Last"]
        col_widths = [150, 40, 44, 46, 44, 40, 58]
        end_col_start, end_col_end = 1, 5

    num_cols = len(headers)
    # Data rows per page: leave room for header + total row on last page
    max_rows_fit = (BODY_BOTTOM - table_top) // ROW_H
    rows_per_page = max(1, max_rows_fit - 2)  # header + total
    show_total = len(all_sites) > 1
    max_site_pages = 5
    raw_pages = ((len(all_sites) + rows_per_page - 1) // rows_per_page) if rows_per_page else 1
    num_pages = min(max_site_pages, raw_pages)
    sites_not_displayed = max(0, len(all_sites) - num_pages * rows_per_page)

    def _add_site_table(page_sid: str, table_sid: str, sites_chunk: list, add_total: bool) -> None:
        num_rows = 1 + len(sites_chunk) + (1 if add_total else 0)
        tbl_w = sum(col_widths)
        tbl_h = num_rows * ROW_H
        reqs.append({
            "createTable": {
                "objectId": table_sid,
                "elementProperties": {
                    "pageObjectId": page_sid,
                    "size": _sz(tbl_w, tbl_h),
                    "transform": _tf(MARGIN, table_top),
                },
                "rows": num_rows,
                "columns": num_cols,
            }
        })

        def _cell_loc(row, col):
            return {"rowIndex": row, "columnIndex": col}

        def _cell_text(row, col, text):
            reqs.append({"insertText": {"objectId": table_sid,
                         "cellLocation": _cell_loc(row, col),
                         "text": text, "insertionIndex": 0}})

        def _cell_style(row, col, text_len, bold=False, color=None, size=FONT_PT, align=None):
            if text_len > 0:
                s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}}
                f = ["fontSize"]
                if bold:
                    s["bold"] = True
                    f.append("bold")
                if color:
                    s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
                    f.append("foregroundColor")
                if FONT:
                    s["fontFamily"] = FONT
                    f.append("fontFamily")
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_sid, "cellLocation": _cell_loc(row, col),
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": s, "fields": ",".join(f),
                    }
                })
            if align:
                reqs.append({
                    "updateParagraphStyle": {
                        "objectId": table_sid, "cellLocation": _cell_loc(row, col),
                        "textRange": {"type": "ALL"},
                        "style": {"alignment": align},
                        "fields": "alignment",
                    }
                })

        def _cell_bg(row, col, color):
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_sid,
                    "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                    "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                    "fields": "tableCellBackgroundFill",
                }
            })

        _clean_table(reqs, table_sid, num_rows, num_cols)

        for ci, h in enumerate(headers):
            _cell_text(0, ci, h)
            _cell_style(0, ci, len(h), bold=True, color=GRAY, align="END" if end_col_start <= ci <= end_col_end else None)
            _cell_bg(0, ci, WHITE)

        for ri, s in enumerate(sites_chunk):
            row = ri + 1
            vals = [
                _short_site(s["sitename"]),
                (s.get("entity", "") or "")[:14] if has_entity else None,
                f'{s["visitors"]:,}',
                f'{s["page_views"]:,}',
                f'{s["feature_clicks"]:,}',
                f'{s["total_events"]:,}',
                f'{s["total_minutes"]:,}',
                (s.get("last_active") or "")[:10],
            ]
            if not has_entity:
                vals.pop(1)
            for ci, v in enumerate(vals):
                _cell_text(row, ci, v)
                _cell_style(row, ci, len(v), color=NAVY, align="END" if end_col_start <= ci <= end_col_end else None)
                _cell_bg(row, ci, WHITE)

        if add_total:
            total_row_idx = len(sites_chunk) + 1
            reqs.append({
                "updateTableBorderProperties": {
                    "objectId": table_sid,
                    "tableRange": {
                        "location": {"rowIndex": total_row_idx, "columnIndex": 0},
                        "rowSpan": 1, "columnSpan": num_cols,
                    },
                    "borderPosition": "TOP",
                    "tableBorderProperties": {
                        "tableBorderFill": {"solidFill": {"color": {"rgbColor": NAVY}}},
                        "weight": {"magnitude": 0.5, "unit": "PT"},
                        "dashStyle": "SOLID",
                    },
                    "fields": "tableBorderFill,weight,dashStyle",
                }
            })
            totals = [
                "Total",
                "" if has_entity else None,
                f'{sum(s["visitors"] for s in all_sites):,}',
                f'{sum(s["page_views"] for s in all_sites):,}',
                f'{sum(s["feature_clicks"] for s in all_sites):,}',
                f'{sum(s["total_events"] for s in all_sites):,}',
                f'{sum(s["total_minutes"] for s in all_sites):,}',
                "",
            ]
            if not has_entity:
                totals.pop(1)
            for ci, v in enumerate(totals):
                text = v if v is not None else ""
                if text or ci == 0:
                    _cell_text(total_row_idx, ci, text)
                _cell_style(total_row_idx, ci, len(text), bold=True, color=NAVY, align="END" if end_col_start <= ci <= end_col_end else None)
                _cell_bg(total_row_idx, ci, WHITE)

    for page in range(num_pages):
        page_sid = f"{sid}_p{page}" if num_pages > 1 else sid
        _slide(reqs, page_sid, idx + page)
        title = f"Site Comparison ({page + 1} of {num_pages})" if num_pages > 1 else "Site Comparison"
        _slide_title(reqs, page_sid, title)

        start = page * rows_per_page
        chunk = all_sites[start : start + rows_per_page]
        add_total = show_total and (page == num_pages - 1)
        _add_site_table(page_sid, f"{page_sid}_table", chunk, add_total)

        # On 5th page when we capped: note how many sites weren't shown
        if page == num_pages - 1 and sites_not_displayed > 0:
            note = f"{sites_not_displayed:,} sites not displayed"
            note_oid = f"{page_sid}_sites_omit"
            _box(reqs, note_oid, page_sid, MARGIN, BODY_BOTTOM - 2, CONTENT_W, 14, note)
            _style(reqs, note_oid, 0, len(note), size=7, color=GRAY, font=FONT, italic=True)

    return idx + num_pages


def _features_slide(reqs, sid, report, idx):
    pages = report["top_pages"]
    features = report["top_features"]
    if not pages and not features:
        return _missing_data_slide(reqs, sid, report, idx, "top pages / feature adoption data")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Feature Adoption")

    # Two columns, fewer items and larger font for readability
    max_items = 5
    font_body = 12
    font_header = 14
    col_gap = 24
    col_w = (CONTENT_W - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_w + col_gap
    box_h = BODY_BOTTOM - BODY_Y

    def _render_column(prefix, title, items, name_key, events_key, events_suffix):
        lines = [title]
        for i, it in enumerate(items[:max_items], 1):
            nm = (it[name_key] or "")[:32]
            if len(it.get(name_key) or "") > 32:
                nm = nm.rstrip() + "…"
            lines.append(f"  {i}. {nm}  ({it[events_key]:,} {events_suffix})")
        if not items:
            lines.append("  No data")
        text = "\n".join(lines)
        oid = f"{sid}_{prefix}"
        _box(reqs, oid, sid, left_x if prefix == "pg" else right_x, BODY_Y, col_w, box_h, text)
        _style(reqs, oid, 0, len(text), size=font_body, color=NAVY, font=FONT)
        _style(reqs, oid, 0, len(title), bold=True, size=font_header, color=BLUE)

    _render_column("pg", "Top Pages", pages, "name", "events", "events")
    _render_column("ft", "Top Features", features, "name", "events", "clicks")

    return idx + 1


def _champions_slide(reqs, sid, report, idx):
    all_champions = report["champions"]
    all_at_risk = report["at_risk_users"]
    if not all_champions and not all_at_risk:
        return _missing_data_slide(reqs, sid, report, idx, "champion / at-risk user data")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Champions & At-Risk Users")

    USER_H = 38
    col_gap = 30
    col_w = (CONTENT_W - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_w + col_gap
    max_per_col = (BODY_BOTTOM - BODY_Y - 28) // USER_H  # 28pt for header

    champions = all_champions[:max_per_col]
    at_risk_show = all_at_risk[:max_per_col]

    def _render_users(users, x, label, label_color, detail_fn, prefix):
        y = BODY_Y
        _box(reqs, f"{sid}_{prefix}h", sid, x, y, col_w, 22, label)
        _style(reqs, f"{sid}_{prefix}h", 0, len(label), bold=True, size=14, color=label_color, font=FONT)
        y += 28

        if not users:
            empty = "No active users" if prefix == "c" else "All users active!"
            _box(reqs, f"{sid}_{prefix}e", sid, x, y, col_w, 20, empty)
            _style(reqs, f"{sid}_{prefix}e", 0, len(empty), size=12, color=GRAY, font=FONT, italic=True)
            return

        for ui, u in enumerate(users):
            email = u["email"] or "unknown"
            if len(email) > 28:
                email = email[:25] + "..."
            detail = detail_fn(u)

            _box(reqs, f"{sid}_{prefix}{ui}", sid, x, y, col_w, 18, email)
            _style(reqs, f"{sid}_{prefix}{ui}", 0, len(email), bold=True, size=12, color=NAVY, font=FONT)

            _box(reqs, f"{sid}_{prefix}d{ui}", sid, x + 8, y + 18, col_w - 8, 16, detail)
            _style(reqs, f"{sid}_{prefix}d{ui}", 0, len(detail), size=10, color=GRAY, font=FONT)
            y += USER_H

    def _champ_detail(u):
        return f"{u['role']}  ·  last seen {u['last_visit']}"

    def _risk_detail(u):
        d = f"{int(u['days_inactive'])}d ago" if u["days_inactive"] < 999 else "never"
        return f"{u['role']}  ·  {d}"

    _render_users(champions, left_x, "Champions", BLUE, _champ_detail, "c")
    _render_users(at_risk_show, right_x, "At Risk  (2 wk – 1 yr inactive)", GRAY, _risk_detail, "r")

    omitted: list[str] = []
    if len(all_champions) > max_per_col:
        omitted.append(f"+{len(all_champions) - max_per_col} more champions")
    if len(all_at_risk) > max_per_col:
        omitted.append(f"+{len(all_at_risk) - max_per_col} more at-risk users")
    _omission_note(reqs, sid, omitted, label="Not shown")

    return idx + 1


def _benchmarks_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Peer Benchmarks")

    bench = report["benchmarks"]
    acct = report["account"]
    cust_rate = bench["customer_active_rate"]
    all_med = bench["peer_median_rate"]
    cohort_med = bench.get("cohort_median_rate")
    cohort_n = bench.get("cohort_count", 0)
    cohort_name = bench.get("cohort_name", "")
    use_cohort = cohort_med is not None and cohort_n >= 3
    med_rate = cohort_med if use_cohort else all_med
    delta = cust_rate - med_rate

    # Big number callout — customer rate
    big = f"{cust_rate}%"
    _box(reqs, f"{sid}_big", sid, MARGIN, BODY_Y + 8, 160, 50, big)
    _style(reqs, f"{sid}_big", 0, len(big), bold=True, size=36, color=BLUE, font=FONT)

    sub = "weekly active rate"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y + 58, 160, 20, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=10, color=GRAY, font=FONT)

    # Cohort median (or all-peer if no cohort)
    med_big = f"{med_rate}%"
    _box(reqs, f"{sid}_med", sid, 220, BODY_Y + 8, 160, 50, med_big)
    _style(reqs, f"{sid}_med", 0, len(med_big), bold=True, size=36, color=NAVY, font=FONT)

    if use_cohort:
        medsub = f"{cohort_name} median ({cohort_n})"
    else:
        medsub = f"all-customer median ({bench['peer_count']})"
    _box(reqs, f"{sid}_ms", sid, 220, BODY_Y + 58, 200, 20, medsub)
    _style(reqs, f"{sid}_ms", 0, len(medsub), size=10, color=GRAY, font=FONT)

    # All-customer median (secondary, if cohort is primary)
    if use_cohort:
        all_big = f"{all_med}%"
        _box(reqs, f"{sid}_all", sid, 440, BODY_Y + 8, 160, 50, all_big)
        _style(reqs, f"{sid}_all", 0, len(all_big), bold=True, size=28, color=GRAY, font=FONT)
        allsub = f"all-customer median ({bench['peer_count']})"
        _box(reqs, f"{sid}_as", sid, 440, BODY_Y + 58, 200, 20, allsub)
        _style(reqs, f"{sid}_as", 0, len(allsub), size=9, color=GRAY, font=FONT)

    # Context
    peer_label = cohort_name if use_cohort else "peer"
    lines = [
        f"Delta: {'+' if delta >= 0 else ''}{delta:.0f} percentage points vs {peer_label} median",
        f"Account size: {acct['total_visitors']} users across {acct['total_sites']} sites",
        "",
    ]
    if delta > 15:
        lines.append(f"Engagement significantly exceeds {peer_label} average.")
        lines.append("Strong candidate for case study, reference, or expansion.")
    elif delta > 0:
        lines.append(f"Performing above {peer_label} average.")
        lines.append("Continue strategy; watch for expansion signals.")
    elif delta > -10:
        lines.append(f"Near the {peer_label} average.")
        lines.append("Monitor for downward trend; proactive outreach recommended.")
    else:
        lines.append(f"Significantly below {peer_label} average.")
        lines.append("Recommend re-engagement, executive check-in, training refresh.")

    ctx = "\n".join(lines)
    _box(reqs, f"{sid}_ctx", sid, MARGIN, BODY_Y + 100, CONTENT_W, 180, ctx)
    _style(reqs, f"{sid}_ctx", 0, len(ctx), size=11, color=NAVY, font=FONT)

    return idx + 1


def _exports_slide(reqs, sid, report, idx):
    exports = report.get("exports", report)
    by_feature = exports.get("by_feature", [])
    top_exporters = exports.get("top_exporters", [])
    total = exports.get("total_exports", 0)

    if not by_feature and total == 0:
        return _missing_data_slide(reqs, sid, report, idx, "export / benchmark data")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Export Behavior")

    # Left: export volume by feature
    per_user = exports.get("exports_per_active_user", 0)
    active = exports.get("active_users", 0)
    header = f"{total:,} exports  ·  {per_user}/active user  ·  {active} active users"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=10, color=GRAY, font=FONT)

    max_features = 8
    fl = ["By Feature"]
    for i, f in enumerate(by_feature[:max_features], 1):
        name = f["feature"][:36] if len(f["feature"]) > 36 else f["feature"]
        fl.append(f"  {i}. {name}  ({f['exports']:,})")
    if not by_feature:
        fl.append("  No export data")
    ft = "\n".join(fl)
    _box(reqs, f"{sid}_bf", sid, MARGIN, BODY_Y + 24, 340, 270, ft)
    _style(reqs, f"{sid}_bf", 0, len(ft), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_bf", 0, len("By Feature"), bold=True, size=11, color=BLUE)

    # Right: top exporters
    max_exporters = (BODY_BOTTOM - BODY_Y - 24) // 28 - 2  # 2 lines per user; reserve note
    el = ["Top Exporters"]
    for u in top_exporters[:max_exporters]:
        email = u["email"] or "unknown"
        if len(email) > 32:
            email = email[:29] + "..."
        el.append(f"  {email}")
        el.append(f"    {u['role']}  ·  {u['exports']:,} exports")
    if not top_exporters:
        el.append("  No export users")
    et = "\n".join(el)
    _box(reqs, f"{sid}_te", sid, 400, BODY_Y + 24, 280, 270, et)
    _style(reqs, f"{sid}_te", 0, len(et), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_te", 0, len("Top Exporters"), bold=True, size=11, color=BLUE)

    omitted: list[str] = []
    if len(by_feature) > max_features:
        omitted.append(f"+{len(by_feature) - max_features} more export types")
    if len(top_exporters) > max_exporters:
        omitted.append(f"+{len(top_exporters) - max_exporters} more exporters")
    _omission_note(reqs, sid, omitted, label="Not shown")

    return idx + 1


def _depth_slide(reqs, sid, report, idx):
    depth = report.get("depth", report)
    breakdown = depth.get("breakdown", [])
    if not breakdown:
        return _missing_data_slide(reqs, sid, report, idx, "depth-of-use breakdown data")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Behavioral Depth")

    write_ratio = depth.get("write_ratio", 0)
    total = depth.get("total_feature_events", 0)
    active = depth.get("active_users", 0)
    header = (f"{total:,} feature interactions  ·  {active} active users  ·  "
              f"{write_ratio}% write ratio")
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=10, color=GRAY, font=FONT)

    charts = report.get("_charts")
    chart_embedded = False
    read_e = depth.get("read_events", 0)
    write_e = depth.get("write_events", 0)
    collab_e = depth.get("collab_events", 0)

    if charts:
        try:
            from .charts import embed_chart

            # Stacked bar: top categories by read/write/collab
            top = breakdown[:8]
            labels = [b["category"] for b in top]
            read_vals = [b.get("read", 0) for b in top]
            write_vals = [b.get("write", 0) for b in top]
            collab_vals = [b.get("collab", 0) for b in top]
            has_rwc = any(v > 0 for v in read_vals + write_vals + collab_vals)

            if has_rwc:
                ss_id, chart_id = charts.add_bar_chart(
                    title="Feature Category Depth",
                    labels=labels,
                    series={"Read": read_vals, "Write": write_vals, "Collab": collab_vals},
                    horizontal=True,
                    stacked=True,
                )
                embed_chart(reqs, f"{sid}_chart", sid, ss_id, chart_id,
                            MARGIN, BODY_Y + 24, CONTENT_W * 0.6, BODY_BOTTOM - BODY_Y - 30)
                chart_embedded = True

            # Pie: overall read/write/collab split
            if read_e + write_e + collab_e > 0:
                ss_id2, pie_id = charts.add_pie_chart(
                    title="Read / Write / Collab",
                    labels=["Read", "Write", "Collab"],
                    values=[read_e, write_e, collab_e],
                    donut=True,
                )
                pie_x = MARGIN + CONTENT_W * 0.62
                pie_w = CONTENT_W * 0.38
                embed_chart(reqs, f"{sid}_pie", sid, ss_id2, pie_id,
                            pie_x, BODY_Y + 24, pie_w, (BODY_BOTTOM - BODY_Y - 30) * 0.6)
                chart_embedded = True
        except Exception as e:
            logger.warning("Chart embed failed for depth slide, falling back to shapes: %s", e)

    if not chart_embedded:
        # Fallback: shape-based bars
        max_events = max((b["events"] for b in breakdown), default=1)
        bar_max_w = 320
        y = BODY_Y + 28
        bar_h = 16
        spacing = 6
        for i, b in enumerate(breakdown[:10]):
            label = f"{b['category']}  ({b['events']:,}, {b['users']}u)"
            _box(reqs, f"{sid}_l{i}", sid, MARGIN, y, 200, bar_h, label)
            _style(reqs, f"{sid}_l{i}", 0, len(label), size=8, color=NAVY, font=FONT)

            bar_w = max(int(b["events"] / max_events * bar_max_w), 4)
            _rect(reqs, f"{sid}_b{i}", sid, 260, y + 2, bar_w, bar_h - 4, BLUE if i < 3 else NAVY)

            pct_label = f"{b['pct']}%"
            _box(reqs, f"{sid}_p{i}", sid, 260 + bar_w + 6, y, 50, bar_h, pct_label)
            _style(reqs, f"{sid}_p{i}", 0, len(pct_label), size=8, color=GRAY, font=FONT)

            y += bar_h + spacing

        summary = f"Read: {read_e:,}\nWrite: {write_e:,}\nCollab: {collab_e:,}"
        _box(reqs, f"{sid}_rw", sid, 560, BODY_Y + 28, 100, 60, summary)
        _style(reqs, f"{sid}_rw", 0, len(summary), size=9, color=NAVY, font=MONO)
        _style(reqs, f"{sid}_rw", 0, len("Read:"), bold=True, color=BLUE)

    return idx + 1


def _kei_slide(reqs, sid, report, idx):
    kei = report.get("kei", report)
    total_q = kei.get("total_queries", 0)
    if total_q == 0 and not kei.get("users"):
        return _missing_data_slide(reqs, sid, report, idx, "Kei query / user data")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Kei AI Adoption")

    active = kei.get("active_users", 0)
    unique = kei.get("unique_users", 0)
    adoption = kei.get("adoption_rate", 0)
    exec_users = kei.get("executive_users", 0)
    exec_queries = kei.get("executive_queries", 0)

    # Metrics row
    metrics = f"{total_q:,} queries  ·  {unique} users  ·  {adoption}% adoption"
    _box(reqs, f"{sid}_met", sid, MARGIN, BODY_Y, CONTENT_W, 18, metrics)
    _style(reqs, f"{sid}_met", 0, len(metrics), size=10, color=GRAY, font=FONT)

    # Executive highlight pill
    if exec_users > 0:
        exec_text = f"  {exec_users} executives ({exec_queries:,} queries)  "
        _pill(reqs, f"{sid}_exec", sid, MARGIN, BODY_Y + 26, 260, 22, exec_text, BLUE, WHITE)
    else:
        exec_text = "  No executive Kei usage detected  "
        _pill(reqs, f"{sid}_exec", sid, MARGIN, BODY_Y + 26, 260, 22, exec_text, GRAY, WHITE)

    # User list
    users = kei.get("users", [])
    lines = ["Kei Users"]
    for u in users[:8]:
        email = u.get("email", "unknown")
        if len(email) > 30:
            email = email[:27] + "..."
        role = u.get("role", "")
        exec_flag = " *" if u.get("is_executive") else ""
        lines.append(f"  {email}")
        lines.append(f"    {role}{exec_flag}  ·  {u.get('queries', 0):,} queries")
    if not users:
        lines.append("  No Kei usage in this period")
    text = "\n".join(lines)
    _box(reqs, f"{sid}_users", sid, MARGIN, BODY_Y + 58, CONTENT_W, 240, text)
    _style(reqs, f"{sid}_users", 0, len(text), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_users", 0, len("Kei Users"), bold=True, size=11, color=BLUE)

    return idx + 1


def _guides_slide(reqs, sid, report, idx):
    guides = report.get("guides", report)
    total_events = guides.get("total_guide_events", 0)
    if total_events == 0:
        return _missing_data_slide(reqs, sid, report, idx, "guide engagement events")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Guide Engagement")

    seen = guides.get("seen", 0)
    advanced = guides.get("advanced", 0)
    dismissed = guides.get("dismissed", 0)
    reach = guides.get("guide_reach", 0)
    dismiss_rate = guides.get("dismiss_rate", 0)
    advance_rate = guides.get("advance_rate", 0)

    metrics = (f"{seen:,} seen  ·  {advance_rate}% advanced  ·  {dismiss_rate}% dismissed  ·  "
               f"{reach}% of users reached")
    _box(reqs, f"{sid}_met", sid, MARGIN, BODY_Y, CONTENT_W, 18, metrics)
    _style(reqs, f"{sid}_met", 0, len(metrics), size=10, color=GRAY, font=FONT)

    # Advance/dismiss bars
    bar_y = BODY_Y + 28
    total_responses = advanced + dismissed
    if total_responses > 0:
        adv_w = int(advanced / total_responses * 400)
        dis_w = int(dismissed / total_responses * 400)
        _rect(reqs, f"{sid}_adv", sid, MARGIN, bar_y, max(adv_w, 4), 18, BLUE)
        _rect(reqs, f"{sid}_dis", sid, MARGIN + adv_w, bar_y, max(dis_w, 4), 18, GRAY)
        alab = f"Advanced ({advanced:,})"
        _box(reqs, f"{sid}_alab", sid, MARGIN, bar_y + 20, 200, 14, alab)
        _style(reqs, f"{sid}_alab", 0, len(alab), size=8, color=BLUE, font=FONT)
        dlab = f"Dismissed ({dismissed:,})"
        _box(reqs, f"{sid}_dlab", sid, MARGIN + adv_w, bar_y + 20, 200, 14, dlab)
        _style(reqs, f"{sid}_dlab", 0, len(dlab), size=8, color=GRAY, font=FONT)
        bar_y += 42

    # Top guides
    top_guides = guides.get("top_guides", [])
    lines = ["Most Active Guides"]
    for g in top_guides[:6]:
        name = g["guide"]
        if len(name) > 40:
            name = name[:37] + "..."
        lines.append(f"  {name}")
        lines.append(f"    seen {g['seen']}  ·  adv {g['advanced']}  ·  dis {g['dismissed']}")
    if not top_guides:
        lines.append("  No guide interactions")
    text = "\n".join(lines)
    _box(reqs, f"{sid}_guides", sid, MARGIN, bar_y + 4, CONTENT_W, 220, text)
    _style(reqs, f"{sid}_guides", 0, len(text), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_guides", 0, len("Most Active Guides"), bold=True, size=11, color=BLUE)

    return idx + 1


def _custom_slide(reqs, sid, report, idx):
    """Flexible slide renderer for agent-composed content.

    Expects data with:
        title: str
        sections: list of {header: str, body: str}
    """
    title = report.get("title", "")
    sections = report.get("sections", [])
    if not title and not sections:
        return _missing_data_slide(reqs, sid, report, idx, "deck title / section list")

    _slide(reqs, sid, idx)
    if title:
        _slide_title(reqs, sid, title)

    y = BODY_Y
    col_w = CONTENT_W
    if len(sections) == 2:
        col_w = 300
    elif len(sections) >= 3:
        col_w = 195

    for i, sec in enumerate(sections[:3]):
        header = sec.get("header", "")
        body = sec.get("body", "")
        x = MARGIN + i * (col_w + 16)

        if header:
            _box(reqs, f"{sid}_h{i}", sid, x, y, col_w, 18, header)
            _style(reqs, f"{sid}_h{i}", 0, len(header), bold=True, size=11, color=BLUE, font=FONT)

        if body:
            body_y = y + (22 if header else 0)
            _box(reqs, f"{sid}_b{i}", sid, x, body_y, col_w, 280, body)
            _style(reqs, f"{sid}_b{i}", 0, len(body), size=10, color=NAVY, font=FONT)

    return idx + 1


def _jira_slide(reqs, sid, report, idx):
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(reqs, sid, report, idx, "Jira support ticket data")
    jira_base = jira.get("base_url", "")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Support Summary")

    total = jira["total_issues"]
    open_n = jira["open_issues"]
    resolved = jira["resolved_issues"]
    esc = jira["escalated"]
    bugs = jira["open_bugs"]
    days = jira.get("days", 90)

    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    date_range = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
    header = f"{total} support tickets  ·  {date_range}  ·  {open_n} open  ·  {resolved} resolved  ·  {esc} escalated  ·  {bugs} open bugs"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=11, color=NAVY, font=FONT, bold=True)

    sla_lines = []
    ttfr = jira.get("ttfr", {})
    if ttfr.get("measured", 0) > 0:
        parts = [f"First Response:  median {ttfr['median']}  ·  avg {ttfr['avg']}"]
        if ttfr.get("breached"):
            parts.append(f"  ·  {ttfr['breached']} breach{'es' if ttfr['breached'] != 1 else ''}")
        if ttfr.get("waiting"):
            parts.append(f"  ·  {ttfr['waiting']} awaiting")
        sla_lines.append("".join(parts))
    ttr = jira.get("ttr", {})
    if ttr.get("measured", 0) > 0:
        parts = [f"Resolution:  median {ttr['median']}  ·  avg {ttr['avg']}"]
        if ttr.get("breached"):
            parts.append(f"  ·  {ttr['breached']} breach{'es' if ttr['breached'] != 1 else ''}")
        if ttr.get("waiting"):
            parts.append(f"  ·  {ttr['waiting']} unresolved")
        sla_lines.append("".join(parts))
    if sla_lines:
        sla_text = "\n".join(sla_lines)
        _box(reqs, f"{sid}_sla", sid, MARGIN, BODY_Y + 18, CONTENT_W, 12 * len(sla_lines) + 4, sla_text)
        _style(reqs, f"{sid}_sla", 0, len(sla_text), size=9, color=GRAY, font=FONT)
        fr_label = "First Response:"
        fr_end = sla_text.find(fr_label)
        if fr_end >= 0:
            _style(reqs, f"{sid}_sla", fr_end, fr_end + len(fr_label), bold=True, color=NAVY)
        res_label = "Resolution:"
        res_pos = sla_text.find(res_label)
        if res_pos >= 0:
            _style(reqs, f"{sid}_sla", res_pos, res_pos + len(res_label), bold=True, color=NAVY)
        body_offset = 22 + 12 * len(sla_lines)
    else:
        body_offset = 28

    col_gap = 20
    left_x = MARGIN
    left_w = (CONTENT_W - col_gap) // 2
    right_x = MARGIN + left_w + col_gap
    right_w = CONTENT_W - left_w - col_gap
    body_top = BODY_Y + body_offset
    max_y = BODY_BOTTOM

    # ── LEFT COLUMN: By Status, By Priority, Recent Issues ──
    left_y = body_top

    status_items = list(jira.get("by_status", {}).items())
    status_lines = []
    for s, c in status_items[:6]:
        status_lines.append(f"{c:>4}  {s}")
    if len(status_items) > 6:
        other = sum(c for _, c in status_items[6:])
        status_lines.append(f"{other:>4}  Other")
    status_text = "By Status\n" + "\n".join(status_lines)
    st_h = min(12 * (len(status_lines) + 1) + 4, max_y - left_y - 120)
    _box(reqs, f"{sid}_st", sid, left_x, left_y, left_w, st_h, status_text)
    _style(reqs, f"{sid}_st", 0, len(status_text), size=8, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_st", 0, len("By Status"), bold=True, size=9, color=BLUE)
    left_y += st_h + 4

    prio_lines = []
    prio_short = {"Blocker: The platform is completely down": "Blocker",
                  "Critical: Significant operational impact": "Critical",
                  "Major: Workaround available, not essential": "Major",
                  "Minor: Impairs non-essential functionality": "Minor"}
    prio_items = list(jira.get("by_priority", {}).items())
    for p, c in prio_items[:5]:
        prio_lines.append(f"{c:>4}  {prio_short.get(p, p[:20])}")
    if len(prio_items) > 5:
        other_p = sum(c for _, c in prio_items[5:])
        prio_lines.append(f"{other_p:>4}  Other")
    prio_text = "By Priority\n" + "\n".join(prio_lines)
    pr_h = min(12 * (len(prio_lines) + 1) + 4, max_y - left_y - 60)
    _box(reqs, f"{sid}_pr", sid, left_x, left_y, left_w, pr_h, prio_text)
    _style(reqs, f"{sid}_pr", 0, len(prio_text), size=8, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_pr", 0, len("By Priority"), bold=True, size=9, color=BLUE)
    left_y += pr_h + 4

    recent = jira.get("recent_issues", [])
    avail_lines = max((max_y - left_y) // 12 - 1, 2)
    recent = recent[:min(avail_lines, 6)]
    recent_lines = []
    for r in recent:
        recent_lines.append(f"{r['key']}  {r['status'][:8]:8s}  {r['summary'][:30]}")
    recent_text = "Recent Issues\n" + "\n".join(recent_lines)
    _box(reqs, f"{sid}_rc", sid, left_x, left_y, left_w, max_y - left_y, recent_text)
    _style(reqs, f"{sid}_rc", 0, len(recent_text), size=8, color=NAVY, font=MONO)
    _style(reqs, f"{sid}_rc", 0, len("Recent Issues"), bold=True, size=9, color=BLUE, font=FONT)
    if jira_base:
        offset = len("Recent Issues\n")
        for r in recent:
            key = r["key"]
            _style(reqs, f"{sid}_rc", offset, offset + len(key), bold=True, size=8,
                   color=BLUE, font=MONO, link=f"{jira_base}/browse/{key}")
            offset += len(f"{key}  {r['status'][:8]:8s}  {r['summary'][:30]}") + 1

    # ── RIGHT COLUMN: Escalated, Engineering Pipeline ──
    right_y = body_top

    esc_issues = jira.get("escalated_issues", [])
    if esc_issues or esc > 0:
        esc_show = esc_issues[:4]
        esc_lines = [f"{e['key']}  {e['summary'][:36]}  ({e['status']})" for e in esc_show]
        esc_text = f"Escalated ({esc})\n" + "\n".join(esc_lines)
        esc_h = 12 * (len(esc_lines) + 1) + 6
        _box(reqs, f"{sid}_esc", sid, right_x, right_y, right_w, esc_h, esc_text)
        _style(reqs, f"{sid}_esc", 0, len(esc_text), size=8, color=NAVY, font=FONT)
        esc_hdr = f"Escalated ({esc})"
        _style(reqs, f"{sid}_esc", 0, len(esc_hdr), bold=True, size=9,
               color={"red": 0.85, "green": 0.15, "blue": 0.15})
        if jira_base:
            offset = len(esc_hdr) + 1
            for e in esc_show:
                key = e["key"]
                _style(reqs, f"{sid}_esc", offset, offset + len(key), bold=True, size=8,
                       color={"red": 0.85, "green": 0.15, "blue": 0.15},
                       link=f"{jira_base}/browse/{key}")
                offset += len(f"{key}  {e['summary'][:36]}  ({e['status']})") + 1
        right_y += esc_h + 4

    eng = jira.get("engineering", {})
    eng_open = eng.get("open", [])
    eng_closed = eng.get("recent_closed", [])
    if eng_open or eng_closed:
        eng_hdr = f"Engineering Pipeline  ({eng.get('open_count', 0)} open · {eng.get('closed_count', 0)} closed)"
        eng_lines = [eng_hdr]
        avail_eng = max((max_y - right_y) // 12 - 2, 2)
        open_show = min(len(eng_open), max(avail_eng - 2, 1))
        for t in eng_open[:open_show]:
            assignee = t.get("assignee") or "unassigned"
            eng_lines.append(f"  {t['key']}  {t['summary'][:26]}  [{assignee}]")
        remaining = avail_eng - open_show
        if eng_closed and remaining > 1:
            eng_lines.append("Recently Closed")
            for t in eng_closed[:min(remaining - 1, 4)]:
                eng_lines.append(f"  {t['key']}  {t['summary'][:36]}")
        eng_text = "\n".join(eng_lines)
        _box(reqs, f"{sid}_eng", sid, right_x, right_y, right_w, max_y - right_y, eng_text)
        _style(reqs, f"{sid}_eng", 0, len(eng_text), size=8, color=NAVY, font=MONO)
        _style(reqs, f"{sid}_eng", 0, len(eng_hdr), bold=True, size=9, color=BLUE, font=FONT)
        rc_start = eng_text.find("Recently Closed")
        if rc_start >= 0:
            _style(reqs, f"{sid}_eng", rc_start, rc_start + len("Recently Closed"),
                   bold=True, size=8, color=GRAY, font=FONT)

    return idx + 1


def _signals_slide(reqs, sid, report, idx):
    signals = report.get("signals", [])
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, "action signals")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Notable Signals")

    max_signals = (BODY_BOTTOM - BODY_Y) // 32 - 1  # ~32pt per signal; reserve note
    shown = signals[:max_signals]
    lines = []
    for i, s in enumerate(shown, 1):
        lines.append(f"{i}.   {s}")
        lines.append("")
    text = "\n".join(lines)

    _box(reqs, f"{sid}_sig", sid, MARGIN, BODY_Y, CONTENT_W, 290, text)
    _style(reqs, f"{sid}_sig", 0, len(text), size=12, color=NAVY, font=FONT)

    # Bold just the number prefix of each signal
    off = 0
    for line in lines:
        if line and line[0].isdigit():
            dot = line.index(".")
            _style(reqs, f"{sid}_sig", off, off + dot + 1, bold=True, color=BLUE)
        off += len(line) + 1

    if len(signals) > max_signals:
        _omission_note(reqs, sid, [f"+{len(signals) - max_signals} more signals"], label="Not shown")

    return idx + 1


# ── Portfolio slide builders (cross-customer) ──


def _portfolio_title_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    n = report.get("customer_count", 0)
    days = report.get("days", 30)
    ql = report.get("quarter")
    title = "Book of Business Review"
    sub = f"{n} customers  ·  {_date_range(days, ql, report.get('quarter_start'), report.get('quarter_end'))}"

    _box(reqs, f"{sid}_t", sid, MARGIN, 100, CONTENT_W, 80, title)
    _style(reqs, f"{sid}_t", 0, len(title), bold=True, size=36, color=WHITE, font=FONT_SERIF)

    _box(reqs, f"{sid}_s", sid, MARGIN, 190, CONTENT_W, 30, sub)
    _style(reqs, f"{sid}_s", 0, len(sub), size=15, color=LTBLUE, font=FONT)

    gen = report.get("generated", "")
    if gen:
        _box(reqs, f"{sid}_d", sid, MARGIN, 340, CONTENT_W, 20, gen)
        _style(reqs, f"{sid}_d", 0, len(gen), size=10, color=GRAY, font=FONT)

    return idx + 1


def _portfolio_signals_slide(reqs, sid, report, idx):
    signals = report.get("portfolio_signals", [])
    if not signals:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio action signals")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Critical Signals Across Portfolio")

    y = BODY_Y
    max_rows = 12
    for i, s in enumerate(signals[:max_rows]):
        sev = s.get("severity", 0)
        dot = "\u25cf "
        dot_color = {"red": 0.85, "green": 0.15, "blue": 0.15} if sev >= 2 else \
                    {"red": 0.9, "green": 0.65, "blue": 0.0}

        cust = s["customer"]
        sig = s["signal"]
        line = f"{dot}{cust}:  {sig}"

        _box(reqs, f"{sid}_r{i}", sid, MARGIN, y, CONTENT_W, 20, line)
        _style(reqs, f"{sid}_r{i}", 0, len(line), size=9, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_r{i}", 0, len(dot), color=dot_color, size=10)
        _style(reqs, f"{sid}_r{i}", len(dot), len(dot) + len(cust), bold=True, size=9)

        y += 22

    return idx + 1


def _portfolio_trends_slide(reqs, sid, report, idx):
    trends_data = report.get("portfolio_trends", {})
    trends = trends_data.get("trends", [])
    if not trends:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio trends")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Aggregate Trends")

    total_active = trends_data.get("total_active_users", 0)
    total_users = trends_data.get("total_users", 0)
    login_pct = trends_data.get("overall_login_pct", 0)
    header = f"{total_active:,} active users of {total_users:,} total  ·  {login_pct}% login rate"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 20, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=12, color=NAVY, font=FONT, bold=True)

    type_colors = {
        "concern": {"red": 0.85, "green": 0.15, "blue": 0.15},
        "opportunity": BLUE,
        "positive": {"red": 0.1, "green": 0.6, "blue": 0.2},
        "insight": NAVY,
    }

    y = BODY_Y + 36
    for i, t in enumerate(trends[:8]):
        trend_type = t.get("type", "insight")
        badge = f"[{trend_type.upper()}]"
        text = t["trend"]
        custs = t.get("customers", "")
        line = f"{badge}  {text}"
        if custs:
            line += f"\n     {custs}"

        _box(reqs, f"{sid}_t{i}", sid, MARGIN, y, CONTENT_W, 34, line)
        _style(reqs, f"{sid}_t{i}", 0, len(line), size=10, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_t{i}", 0, len(badge), bold=True, size=10,
               color=type_colors.get(trend_type, NAVY))

        if custs:
            cust_start = line.index(custs)
            _style(reqs, f"{sid}_t{i}", cust_start, cust_start + len(custs),
                   size=8, color=GRAY)

        y += 38

    return idx + 1


def _portfolio_leaders_slide(reqs, sid, report, idx):
    leaders = report.get("portfolio_leaders", {})
    if not leaders:
        return _missing_data_slide(reqs, sid, report, idx, "portfolio leaders")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Customer Leaders")

    categories = [
        ("kei_adoption", "Kei AI Adoption", "adoption_rate", "%"),
        ("executive_engagement", "Executive Engagement", "executives", ""),
        ("engagement_score", "Engagement Score", "score", ""),
        ("write_depth", "Write Depth", "write_ratio", "%"),
        ("export_intensity", "Export Volume", "total_exports", ""),
        ("login_rate", "Login Rate", "login_pct", "%"),
    ]

    col_w = (CONTENT_W - 20) // 3
    col_h = 150
    positions = [
        (MARGIN, BODY_Y),
        (MARGIN + col_w + 10, BODY_Y),
        (MARGIN + 2 * (col_w + 10), BODY_Y),
        (MARGIN, BODY_Y + col_h + 10),
        (MARGIN + col_w + 10, BODY_Y + col_h + 10),
        (MARGIN + 2 * (col_w + 10), BODY_Y + col_h + 10),
    ]

    for ci, (key, label, metric, unit) in enumerate(categories):
        entries = leaders.get(key, [])
        if not entries or ci >= len(positions):
            continue
        x, y = positions[ci]

        _rect(reqs, f"{sid}_bg{ci}", sid, x, y, col_w, col_h, LIGHT)

        _box(reqs, f"{sid}_cat{ci}", sid, x + 8, y + 6, col_w - 16, 18, label)
        _style(reqs, f"{sid}_cat{ci}", 0, len(label), bold=True, size=10, color=BLUE, font=FONT)

        lines = []
        for e in entries[:5]:
            val = e.get(metric, 0)
            if isinstance(val, float):
                val = round(val)
            lines.append(f"{e['rank']}.  {e['customer']}  —  {val}{unit}")
        text = "\n".join(lines)

        _box(reqs, f"{sid}_ent{ci}", sid, x + 8, y + 28, col_w - 16, col_h - 34, text)
        _style(reqs, f"{sid}_ent{ci}", 0, len(text), size=9, color=NAVY, font=FONT)

        off = 0
        for line in lines:
            dot_end = line.index(".")
            _style(reqs, f"{sid}_ent{ci}", off, off + dot_end + 1, bold=True, color=BLUE, size=9)
            off += len(line) + 1

    return idx + 1


# ── Data Quality slide ──

_GREEN = {"red": 0.13, "green": 0.65, "blue": 0.35}   # #21a659
_AMBER = {"red": 0.9,  "green": 0.65, "blue": 0.0}    # #e6a600
_RED   = {"red": 0.85, "green": 0.15, "blue": 0.15}    # #d92626

_SEV_COLOR = {"ERROR": _RED, "WARNING": _AMBER, "INFO": GRAY}
_SEV_DOT   = {"ERROR": "\u2716", "WARNING": "\u26a0", "INFO": "\u2139"}


def _data_quality_slide(reqs, sid, report, idx):
    from .qa import qa
    snap = qa.summary(report=report)

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Data Quality")

    # ── Section 1: Data source status badges ──
    sources = snap.get("data_sources", {})
    src_x = MARGIN
    src_y = BODY_Y
    for si, (name, status) in enumerate(sources.items()):
        if status == "ok":
            icon, color = "\u2713", _GREEN
        else:
            icon, color = "\u2717", _AMBER
        label = f"{icon} {name}"
        _pill(reqs, f"{sid}_src{si}", sid, src_x, src_y, 120, 22, label, WHITE, color)
        src_x += 130

    # ── Section 2: Validation summary ──
    total_checks = snap["total_checks"]
    total_flags = snap["total_flags"]
    n_errors = snap["errors"]
    n_warnings = snap["warnings"]

    sum_y = src_y + 36

    if total_flags == 0:
        status = f"All {total_checks} cross-source checks passed"
        status_color = _GREEN
    elif n_errors > 0:
        status = f"{n_errors} error{'s' if n_errors != 1 else ''} and {n_warnings} warning{'s' if n_warnings != 1 else ''} found"
        status_color = _RED
    else:
        status = f"{n_warnings} finding{'s' if n_warnings != 1 else ''} to note"
        status_color = _AMBER

    _box(reqs, f"{sid}_st", sid, MARGIN, sum_y, CONTENT_W, 20, status)
    _style(reqs, f"{sid}_st", 0, len(status), bold=True, size=12, color=status_color, font=FONT)

    # ── Section 3: Findings (customer-facing flags only) ──
    y = sum_y + 28
    max_rows = 10
    flags = snap["flags"]
    sorted_flags = sorted(flags, key=lambda f: {"ERROR": 0, "WARNING": 1, "INFO": 2}.get(f["severity"], 3))

    for i, f in enumerate(sorted_flags[:max_rows]):
        sev = f["severity"]
        dot = _SEV_DOT.get(sev, "?")
        dot_color = _SEV_COLOR.get(sev, GRAY)

        msg = f["message"]
        detail_parts = []
        if f["expected"] is not None and f["actual"] is not None:
            detail_parts.append(f"expected {f['expected']}, got {f['actual']}")
        if f["sources"]:
            detail_parts.append(" vs ".join(f["sources"]))

        line = f"{dot}  {msg}"
        detail = ""
        if detail_parts:
            detail = f"    {' · '.join(detail_parts)}"

        full = line + detail
        if len(full) > 120:
            full = full[:117] + "..."

        _box(reqs, f"{sid}_f{i}", sid, MARGIN, y, CONTENT_W, 18, full)
        _style(reqs, f"{sid}_f{i}", 0, len(full), size=9, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_f{i}", 0, len(dot), color=dot_color, size=10, bold=True)
        if detail:
            _style(reqs, f"{sid}_f{i}", len(line), len(full), color=GRAY, size=8)

        y += 20

    if len(flags) > max_rows:
        more = f"... and {len(flags) - max_rows} more"
        _box(reqs, f"{sid}_more", sid, MARGIN, y, CONTENT_W, 16, more)
        _style(reqs, f"{sid}_more", 0, len(more), size=8, color=GRAY, font=FONT, italic=True)
        y += 18

    # ── Section 4: Confidence note ──
    note_y = max(y + 6, BODY_BOTTOM - 40)
    note = ("Single-source metrics (feature adoption, exports, guides, dollar values) "
            "are not independently verified across sources.")
    _box(reqs, f"{sid}_note", sid, MARGIN, note_y, CONTENT_W, 28, note)
    _style(reqs, f"{sid}_note", 0, len(note), size=7, color=GRAY, font=FONT, italic=True)

    return idx + 1


# ── CS Report slide builders ──

_HEALTH_BADGE = {
    "GREEN": ({"red": 0.10, "green": 0.55, "blue": 0.28}, "\u2705"),
    "YELLOW": ({"red": 0.9, "green": 0.65, "blue": 0.0}, "\u26a0"),
    "RED": ({"red": 0.78, "green": 0.18, "blue": 0.18}, "\u2716"),
}


def _platform_health_slide(reqs, sid, report, idx):
    cs = report.get("cs_platform_health", report)
    site_list = cs.get("sites", [])
    if not site_list:
        return _missing_data_slide(reqs, sid, report, idx, "CS Report platform health / site list")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Platform Health")

    dist = cs.get("health_distribution", {})
    total_short = cs.get("total_shortages", 0)
    total_crit = cs.get("total_critical_shortages", 0)
    header = "  ·  ".join(
        [f"{v} {k}" for k, v in dist.items() if v > 0]
        + [f"{total_short:,} shortages ({total_crit:,} critical)"]
    )
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=10, color=GRAY, font=FONT)

    ROW_H = 28
    max_rows = (BODY_BOTTOM - BODY_Y - 24) // ROW_H - 1  # reserve space for omission note
    headers_list = ["Factory", "Health", "CTB%", "CTC%", "Comp Avail%", "Shortages", "Critical"]
    col_widths = [170, 60, 55, 55, 75, 65, 60]
    show = site_list[:max_rows]
    omitted_factories = [s.get("factory", "?") for s in site_list[max_rows:]]
    num_rows = 1 + len(show)
    table_id = f"{sid}_tbl"

    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * ROW_H),
                "transform": _tf(MARGIN, BODY_Y + 24),
            },
            "rows": num_rows, "columns": len(headers_list),
        }
    })

    def _ct(row, col, text):
        if not text:
            return
        reqs.append({"insertText": {"objectId": table_id,
                     "cellLocation": {"rowIndex": row, "columnIndex": col},
                     "text": text, "insertionIndex": 0}})

    def _cs(row, col, text_len, bold=False, color=None, size=8, align=None):
        if text_len > 0:
            s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
            f = ["fontSize", "fontFamily"]
            if bold:
                s["bold"] = True; f.append("bold")
            if color:
                s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
            reqs.append({
                "updateTextStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                    "style": s, "fields": ",".join(f),
                }
            })
        if align:
            reqs.append({
                "updateParagraphStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "ALL"},
                    "style": {"alignment": align}, "fields": "alignment",
                }
            })

    def _cbg(row, col, color):
        reqs.append({
            "updateTableCellProperties": {
                "objectId": table_id,
                "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                "fields": "tableCellBackgroundFill",
            }
        })

    _clean_table(reqs, table_id, num_rows, len(headers_list))

    for ci, h in enumerate(headers_list):
        _ct(0, ci, h)
        _cs(0, ci, len(h), bold=True, color=GRAY, size=8, align="END" if ci >= 2 else None)
        _cbg(0, ci, WHITE)

    for ri, s in enumerate(show):
        row = ri + 1
        hs = s.get("health_score") or "NONE"
        badge_info = _HEALTH_BADGE.get(hs)
        badge = badge_info[1] + " " + hs if badge_info else hs
        vals = [
            s.get("factory", "?")[:24],
            badge,
            f'{s.get("clear_to_build_pct", 0):.1f}' if "clear_to_build_pct" in s else "-",
            f'{s.get("clear_to_commit_pct", 0):.1f}' if "clear_to_commit_pct" in s else "-",
            f'{s.get("component_availability_pct", 0):.1f}' if "component_availability_pct" in s else "-",
            f'{s.get("shortages", 0):,}' if "shortages" in s else "-",
            f'{s.get("critical_shortages", 0):,}' if "critical_shortages" in s else "-",
        ]
        for ci, v in enumerate(vals):
            _ct(row, ci, v)
            _cs(row, ci, len(v), color=NAVY, size=8, align="END" if ci >= 2 else None)
            _cbg(row, ci, WHITE)

    _omission_note(reqs, sid, omitted_factories, label="Not shown")

    return idx + 1


def _supply_chain_slide(reqs, sid, report, idx):
    cs = report.get("cs_supply_chain", report)
    site_list = cs.get("sites", [])
    if not site_list:
        return _missing_data_slide(reqs, sid, report, idx, "CS Report supply chain / site list")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Supply Chain Overview")

    totals = cs.get("totals", {})
    oh = totals.get("on_hand", 0)
    oo = totals.get("on_order", 0)
    ex = totals.get("excess_on_hand", 0)
    header = f"${oh:,.0f} on-hand  ·  ${oo:,.0f} on-order  ·  ${ex:,.0f} excess"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), bold=True, size=11, color=NAVY, font=FONT)

    ROW_H = 28
    max_rows = (BODY_BOTTOM - BODY_Y - 28) // ROW_H - 1  # reserve space for omission note
    headers_list = ["Factory", "On-Hand", "On-Order", "Excess", "DOI", "Late POs"]
    col_widths = [150, 90, 90, 80, 55, 55]
    show = site_list[:max_rows]
    omitted_factories = [s.get("factory", "?") for s in site_list[max_rows:]]
    num_rows = 1 + len(show)
    table_id = f"{sid}_tbl"

    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(sum(col_widths), num_rows * ROW_H),
                "transform": _tf(MARGIN, BODY_Y + 28),
            },
            "rows": num_rows, "columns": len(headers_list),
        }
    })

    def _ct(row, col, text):
        if not text:
            return
        reqs.append({"insertText": {"objectId": table_id,
                     "cellLocation": {"rowIndex": row, "columnIndex": col},
                     "text": text, "insertionIndex": 0}})

    def _cs(row, col, text_len, bold=False, color=None, size=8, align=None):
        if text_len > 0:
            s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
            f = ["fontSize", "fontFamily"]
            if bold:
                s["bold"] = True; f.append("bold")
            if color:
                s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
            reqs.append({
                "updateTextStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                    "style": s, "fields": ",".join(f),
                }
            })
        if align:
            reqs.append({
                "updateParagraphStyle": {
                    "objectId": table_id,
                    "cellLocation": {"rowIndex": row, "columnIndex": col},
                    "textRange": {"type": "ALL"},
                    "style": {"alignment": align}, "fields": "alignment",
                }
            })

    def _cbg(row, col, color):
        reqs.append({
            "updateTableCellProperties": {
                "objectId": table_id,
                "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                "fields": "tableCellBackgroundFill",
            }
        })

    _clean_table(reqs, table_id, num_rows, len(headers_list))

    for ci, h in enumerate(headers_list):
        _ct(0, ci, h)
        _cs(0, ci, len(h), bold=True, color=GRAY, size=8, align="END" if ci >= 1 else None)
        _cbg(0, ci, WHITE)

    def _fmtk(v):
        if v is None or v == 0:
            return "-"
        if abs(v) >= 1_000_000:
            return f"${v/1_000_000:.1f}M"
        if abs(v) >= 1_000:
            return f"${v/1_000:.0f}K"
        return f"${v:,.0f}"

    for ri, s in enumerate(show):
        row = ri + 1
        vals = [
            s.get("factory", "?")[:22],
            _fmtk(s.get("on_hand_value")),
            _fmtk(s.get("on_order_value")),
            _fmtk(s.get("excess_on_hand")),
            f'{s["doi_days"]:.0f}d' if "doi_days" in s else "-",
            f'{s.get("late_pos", 0):,}' if "late_pos" in s else "-",
        ]
        for ci, v in enumerate(vals):
            _ct(row, ci, v)
            _cs(row, ci, len(v), color=NAVY, size=8, align="END" if ci >= 1 else None)
            _cbg(row, ci, WHITE)

    _omission_note(reqs, sid, omitted_factories, label="Not shown")

    return idx + 1


def _platform_value_slide(reqs, sid, report, idx):
    cs = report.get("cs_platform_value", report)
    total_savings = cs.get("total_savings", 0)
    total_open = cs.get("total_open_ia_value", 0)
    total_recs = cs.get("total_recs_created_30d", 0)
    site_list = cs.get("sites", [])

    if total_savings == 0 and total_open == 0 and total_recs == 0:
        return _missing_data_slide(reqs, sid, report, idx, "platform value / ROI (savings, pipeline, recommendations)")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Platform Value & ROI")

    def _fmt_dollar(v):
        if abs(v) >= 1_000_000_000:
            return f"${v / 1_000_000_000:,.2f}B"
        if abs(v) >= 1_000_000:
            return f"${v / 1_000_000:,.1f}M"
        if abs(v) >= 1_000:
            return f"${v / 1_000:,.0f}K"
        return f"${v:,.0f}"

    def _fmt_count(v):
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:,.1f}M"
        if abs(v) >= 100_000:
            return f"{v / 1_000:,.0f}K"
        return f"{v:,}"

    sav = _fmt_dollar(total_savings)
    _box(reqs, f"{sid}_sav", sid, MARGIN, BODY_Y + 8, 200, 50, sav)
    _style(reqs, f"{sid}_sav", 0, len(sav), bold=True, size=28, color=BLUE, font=FONT)

    sav_sub = "savings achieved"
    _box(reqs, f"{sid}_ss", sid, MARGIN, BODY_Y + 56, 200, 18, sav_sub)
    _style(reqs, f"{sid}_ss", 0, len(sav_sub), size=9, color=GRAY, font=FONT)

    opn = _fmt_dollar(total_open)
    _box(reqs, f"{sid}_opn", sid, 260, BODY_Y + 8, 200, 50, opn)
    _style(reqs, f"{sid}_opn", 0, len(opn), bold=True, size=28, color=NAVY, font=FONT)

    opn_sub = "open IA pipeline"
    _box(reqs, f"{sid}_os", sid, 260, BODY_Y + 56, 200, 18, opn_sub)
    _style(reqs, f"{sid}_os", 0, len(opn_sub), size=9, color=GRAY, font=FONT)

    recs_text = _fmt_count(total_recs)
    _box(reqs, f"{sid}_recs", sid, 480, BODY_Y + 8, 160, 50, recs_text)
    _style(reqs, f"{sid}_recs", 0, len(recs_text), bold=True, size=28, color=TEAL, font=FONT)

    recs_sub = "recs created (30d)"
    _box(reqs, f"{sid}_rs", sid, 480, BODY_Y + 56, 160, 18, recs_sub)
    _style(reqs, f"{sid}_rs", 0, len(recs_sub), size=9, color=GRAY, font=FONT)

    # Per-site breakdown
    total_pos = cs.get("total_pos_placed_30d", 0)
    total_overdue = cs.get("total_overdue_tasks", 0)
    ops = f"{total_pos:,} POs placed  ·  {total_overdue:,} overdue tasks"
    _box(reqs, f"{sid}_ops", sid, MARGIN, BODY_Y + 84, CONTENT_W, 16, ops)
    _style(reqs, f"{sid}_ops", 0, len(ops), size=9, color=GRAY, font=FONT)

    # Factory breakdown as a table
    factory_rows = [s for s in site_list if s.get("savings_current_period") or s.get("recs_created_30d")]
    if factory_rows:
        tbl_y = BODY_Y + 108
        ROW_H = 28
        max_rows = (BODY_BOTTOM - tbl_y) // ROW_H - 1  # reserve note
        show = factory_rows[:max_rows]
        omitted_factories = [s.get("factory", "?") for s in factory_rows[max_rows:]]
        headers_list = ["Factory", "Savings", "Recs (30d)"]
        col_widths = [180, 120, 80]
        num_rows = 1 + len(show)
        table_id = f"{sid}_tbl"

        reqs.append({
            "createTable": {
                "objectId": table_id,
                "elementProperties": {
                    "pageObjectId": sid,
                    "size": _sz(sum(col_widths), num_rows * ROW_H),
                    "transform": _tf(MARGIN, tbl_y),
                },
                "rows": num_rows, "columns": len(headers_list),
            }
        })

        def _ct(row, col, text):
            if not text:
                return
            reqs.append({"insertText": {"objectId": table_id,
                         "cellLocation": {"rowIndex": row, "columnIndex": col},
                         "text": text, "insertionIndex": 0}})

        def _cs(row, col, text_len, bold=False, color=None, size=8, align=None):
            if text_len > 0:
                s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}, "fontFamily": FONT}
                f = ["fontSize", "fontFamily"]
                if bold:
                    s["bold"] = True; f.append("bold")
                if color:
                    s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
                reqs.append({
                    "updateTextStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                        "style": s, "fields": ",".join(f),
                    }
                })
            if align:
                reqs.append({
                    "updateParagraphStyle": {
                        "objectId": table_id,
                        "cellLocation": {"rowIndex": row, "columnIndex": col},
                        "textRange": {"type": "ALL"},
                        "style": {"alignment": align}, "fields": "alignment",
                    }
                })

        def _cbg(row, col, color):
            reqs.append({
                "updateTableCellProperties": {
                    "objectId": table_id,
                    "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                    "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                    "fields": "tableCellBackgroundFill",
                }
            })

        _clean_table(reqs, table_id, num_rows, len(headers_list))

        for ci, h in enumerate(headers_list):
            _ct(0, ci, h)
            _cs(0, ci, len(h), bold=True, color=GRAY, size=8, align="END" if ci >= 1 else None)
            _cbg(0, ci, WHITE)

        for ri, s in enumerate(show):
            row = ri + 1
            sav_v = s.get("savings_current_period", 0)
            recs_v = s.get("recs_created_30d", 0)
            vals = [
                s.get("factory", "?")[:24],
                f"${sav_v:,.0f}" if sav_v else "-",
                f"{recs_v:,}" if recs_v else "-",
            ]
            for ci, v in enumerate(vals):
                _ct(row, ci, v)
                _cs(row, ci, len(v), color=NAVY, size=8, align="END" if ci >= 1 else None)
                _cbg(row, ci, WHITE)

        _omission_note(reqs, sid, omitted_factories, label="Not shown")

    return idx + 1


# ── Team roster slide ──

def _load_teams() -> dict[str, Any]:
    """Load team rosters from teams.yaml (project root)."""
    import yaml
    path = Path(__file__).resolve().parent.parent / "teams.yaml"
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}


def _team_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)

    customer = report.get("customer", "Customer")
    teams = _load_teams()
    team_data = teams.get(customer, {})
    cust_members = [m.get("name", "") for m in team_data.get("customer_team", [])]
    ldna_members = [m.get("name", "") for m in team_data.get("leandna_team", [])]

    if not cust_members and not ldna_members:
        cust_members = ["(no team roster configured)"]
        ldna_members = ["(no team roster configured)"]

    # Right panel: blue branded area
    panel_x = 310
    panel_w = SLIDE_W - panel_x
    _rect(reqs, f"{sid}_rpanel", sid, panel_x, 0, panel_w, SLIDE_H, BLUE)

    # Gradient overlay: darker navy strip at right edge
    _rect(reqs, f"{sid}_rnav", sid, SLIDE_W - 80, 0, 80, SLIDE_H, NAVY)

    # "LeanDNA.com" text on the blue panel
    brand = "LeanDNA.com"
    _box(reqs, f"{sid}_brand", sid, panel_x + 40, SLIDE_H - 60, 200, 30, brand)
    _style(reqs, f"{sid}_brand", 0, len(brand), bold=True, size=16, color=WHITE, font=FONT)

    # Left panel: white background (default), team rosters
    left_w = panel_x - MARGIN
    y = 30

    # Customer team header
    cust_hdr = f"{customer} Team"
    _box(reqs, f"{sid}_ch", sid, MARGIN, y, left_w, 24, cust_hdr)
    _style(reqs, f"{sid}_ch", 0, len(cust_hdr), bold=True, size=14, color=BLUE, font=FONT)
    y += 30

    # Customer team members
    for i, name in enumerate(cust_members[:12]):
        _box(reqs, f"{sid}_cm{i}", sid, MARGIN, y, left_w, 16, name)
        _style(reqs, f"{sid}_cm{i}", 0, len(name), bold=True, size=10, color=NAVY, font=FONT)
        y += 18

    y += 14

    # LeanDNA team header
    ldna_hdr = "LeanDNA Team"
    _box(reqs, f"{sid}_lh", sid, MARGIN, y, left_w, 24, ldna_hdr)
    _style(reqs, f"{sid}_lh", 0, len(ldna_hdr), bold=True, size=14, color=BLUE, font=FONT)
    y += 30

    # LeanDNA team members
    for i, name in enumerate(ldna_members[:12]):
        _box(reqs, f"{sid}_lm{i}", sid, MARGIN, y, left_w, 16, name)
        _style(reqs, f"{sid}_lm{i}", 0, len(name), bold=True, size=10, color=NAVY, font=FONT)
        y += 18

    return idx + 1


# ── New slides: SLA Health, Cross-Validation, Engineering Pipeline, Enhancement Requests ──


def _sla_health_slide(reqs, sid, report, idx):
    """SLA performance, sentiment distribution, and request type mix. Always appears; shows red banner when no data."""
    jira = report.get("jira")
    if not jira or jira.get("total_issues", 0) == 0:
        return _missing_data_slide(
            reqs, sid, report, idx,
            "Jira support tickets and SLA metrics (no tickets in period or Jira unavailable)",
        )

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Support Health & SLA")

    days = jira.get("days", 90)
    total = jira["total_issues"]
    from datetime import date, timedelta
    end = date.today()
    start = end - timedelta(days=days)
    date_range = f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"

    header = f"{total} tickets  ·  {date_range}"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 20, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=12, color=GRAY, font=FONT)

    col_gap = 24
    left_x = MARGIN
    left_w = (CONTENT_W - col_gap) // 2
    right_x = MARGIN + left_w + col_gap
    right_w = CONTENT_W - left_w - col_gap
    body_top = BODY_Y + 26
    max_y = BODY_BOTTOM

    # ── LEFT: SLA gauges ──
    y = body_top

    sla_goal = {"ttfr": "48h", "ttr": "160h"}
    sla_label = {"ttfr": "First Response", "ttr": "Resolution"}
    for sla_key in ("ttfr", "ttr"):
        sla = jira.get(sla_key, {})
        measured = sla.get("measured", 0)
        if measured == 0:
            continue
        label = sla_label[sla_key]
        goal = sla_goal[sla_key]
        breached = sla.get("breached", 0)
        breach_pct = round(100 * breached / max(measured, 1))

        if breach_pct == 0:
            badge_color = {"red": 0.13, "green": 0.55, "blue": 0.13}
            badge_label = "On track"
        elif breach_pct <= 20:
            badge_color = {"red": 0.85, "green": 0.65, "blue": 0.0}
            badge_label = "Caution"
        else:
            badge_color = {"red": 0.85, "green": 0.15, "blue": 0.15}
            badge_label = "At risk"

        title_text = f"{label}  (goal: {goal})"
        _box(reqs, f"{sid}_{sla_key}_t", sid, left_x, y, left_w, 20, title_text)
        _style(reqs, f"{sid}_{sla_key}_t", 0, len(label), bold=True, size=13, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_{sla_key}_t", len(label), len(title_text), size=10, color=GRAY, font=FONT)
        y += 24

        _pill(reqs, f"{sid}_{sla_key}_b", sid, left_x, y, 88, 22, badge_label, badge_color, WHITE)

        breach_txt = "0 breaches" if breached == 0 else f"{breached} breach{'es' if breached != 1 else ''}"
        stats = f"Median {sla.get('median', '—')}  ·  Avg {sla.get('avg', '—')}  ·  {breach_txt} (of {measured} closed)"
        _box(reqs, f"{sid}_{sla_key}_s", sid, left_x + 96, y, left_w - 96, 22, stats)
        _style(reqs, f"{sid}_{sla_key}_s", 0, len(stats), size=11, color=NAVY, font=FONT)
        y += 26

        if sla.get("min") and sla.get("max"):
            range_text = f"Range {sla['min']} – {sla['max']}"
            if sla.get("waiting"):
                range_text += f"  ·  {sla['waiting']} open"
            _box(reqs, f"{sid}_{sla_key}_r", sid, left_x, y, left_w, 18, range_text)
            _style(reqs, f"{sid}_{sla_key}_r", 0, len(range_text), size=10, color=GRAY, font=FONT)
            y += 22

        y += 14

    # ── RIGHT: Sentiment + Request Type ──
    right_y = body_top

    sentiment = jira.get("by_sentiment", {})
    sentiment_clean = {k: v for k, v in sentiment.items() if k != "Unknown"}
    if sentiment_clean:
        sent_title = "Ticket sentiment"
        _box(reqs, f"{sid}_sent_t", sid, right_x, right_y, right_w, 20, sent_title)
        _style(reqs, f"{sid}_sent_t", 0, len(sent_title), bold=True, size=13, color=NAVY, font=FONT)
        right_y += 24

        color_map = {
            "Positive": {"red": 0.13, "green": 0.55, "blue": 0.13},
            "Neutral": {"red": 0.5, "green": 0.5, "blue": 0.5},
            "Negative": {"red": 0.85, "green": 0.15, "blue": 0.15},
        }
        sent_total = sum(sentiment_clean.values())
        for si, (name, count) in enumerate(sentiment_clean.items()):
            pct = round(100 * count / max(sent_total, 1))
            bar_w = max(int(pct * (right_w - 120) / 100), 6)
            fill = color_map.get(name, GRAY)
            _rect(reqs, f"{sid}_sb{si}", sid, right_x, right_y, bar_w, 16, fill)
            label = f"{name}  {count} ({pct}%)"
            _box(reqs, f"{sid}_sl{si}", sid, right_x + bar_w + 8, right_y, right_w - bar_w - 8, 16, label)
            _style(reqs, f"{sid}_sl{si}", 0, len(label), size=11, color=NAVY, font=FONT)
            right_y += 22
        right_y += 14

    req_types = jira.get("by_request_type", {})
    if req_types:
        rt_title = "Request channels"
        _box(reqs, f"{sid}_rt_t", sid, right_x, right_y, right_w, 20, rt_title)
        _style(reqs, f"{sid}_rt_t", 0, len(rt_title), bold=True, size=13, color=NAVY, font=FONT)
        right_y += 24

        rt_lines = []
        for name, count in list(req_types.items())[:6]:
            rt_lines.append(f"{count:>3}  {name}")
        rt_text = "\n".join(rt_lines)
        rt_h = min(14 * len(rt_lines) + 6, max_y - right_y)
        _box(reqs, f"{sid}_rtl", sid, right_x, right_y, right_w, rt_h, rt_text)
        _style(reqs, f"{sid}_rtl", 0, len(rt_text), size=11, color=NAVY, font=FONT)

    return idx + 1


def _cross_validation_slide(reqs, sid, report, idx):
    """Pendo vs CS Report engagement comparison per site."""
    cs_ph = report.get("cs_platform_health", {})
    pendo_sites = report.get("sites", [])

    cs_factories = cs_ph.get("factories", [])
    if not cs_factories and not pendo_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo sites and/or CS Report factories for comparison")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Data Cross-Validation")

    engagement = report.get("engagement", {})
    pendo_rate = engagement.get("active_rate_7d")
    if pendo_rate is not None:
        pendo_rate = round(pendo_rate)

    header_parts = []
    if pendo_rate is not None:
        header_parts.append(f"Pendo 7-day active rate: {pendo_rate}%")
    cs_buyer_rates = [f["weekly_active_buyers_pct"] for f in cs_factories
                      if f.get("weekly_active_buyers_pct") is not None]
    if cs_buyer_rates:
        cs_avg = round(sum(cs_buyer_rates) / len(cs_buyer_rates))
        header_parts.append(f"CS Report avg active buyers: {cs_avg}%")
    if pendo_rate is not None and cs_buyer_rates:
        diff = abs(pendo_rate - cs_avg)
        if diff <= 15:
            header_parts.append("✓ Consistent")
        else:
            header_parts.append(f"⚠ {diff}pp gap")

    header = "  ·  ".join(header_parts) if header_parts else "Comparing Pendo usage with CS Report metrics"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)

    # Build per-site comparison table
    ROW_H = 20
    tbl_y = BODY_Y + 24
    max_rows = (BODY_BOTTOM - tbl_y) // ROW_H - 1

    pendo_by_site: dict[str, dict] = {}
    for s in pendo_sites:
        name = s.get("sitename") or s.get("site_name", "")
        if name:
            pendo_by_site[name.lower()] = s

    rows: list[tuple[str, str, str, str, str]] = []
    for f in cs_factories:
        fname = f.get("factory_name", "")
        wab = f.get("weekly_active_buyers_pct")
        health = f.get("health_score")
        pendo_match = None
        for pname, ps in pendo_by_site.items():
            if fname.lower() in pname or pname in fname.lower():
                pendo_match = ps
                break

        p_users = str(pendo_match.get("total_visitors", "—")) if pendo_match else "—"
        p_events = _fmt_count(pendo_match.get("total_events", 0)) if pendo_match else "—"
        cs_wab = f"{wab:.0f}%" if wab is not None else "—"
        cs_health_str = f"{health:.0f}" if health is not None else "—"
        rows.append((fname[:22], p_users, p_events, cs_wab, cs_health_str))

    if not rows:
        note = "No overlapping site data between Pendo and CS Report"
        _box(reqs, f"{sid}_none", sid, MARGIN, tbl_y, CONTENT_W, 30, note)
        _style(reqs, f"{sid}_none", 0, len(note), size=11, color=GRAY, font=FONT)
        return idx + 1

    shown = rows[:max_rows]
    cols = ["Site", "Pendo Users", "Pendo Events", "CS Active %", "CS Health"]
    col_widths = [150, 90, 100, 90, 90]
    num_rows = len(shown) + 1
    num_cols = len(cols)
    table_id = f"{sid}_tbl"

    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": {"width": {"magnitude": sum(col_widths), "unit": "PT"},
                         "height": {"magnitude": ROW_H * num_rows, "unit": "PT"}},
                "transform": _tf(MARGIN, tbl_y),
            },
            "rows": num_rows, "columns": num_cols,
        }
    })
    _clean_table(reqs, table_id, num_rows, num_cols)

    for ci, hdr in enumerate(cols):
        reqs.append({"insertText": {"objectId": table_id, "text": hdr,
                                    "cellLocation": {"tableId": table_id, "rowIndex": 0, "columnIndex": ci}}})
    for ri, row in enumerate(shown, 1):
        for ci, val in enumerate(row):
            reqs.append({"insertText": {"objectId": table_id, "text": val,
                                        "cellLocation": {"tableId": table_id, "rowIndex": ri, "columnIndex": ci}}})

    if len(rows) > max_rows:
        _omission_note(reqs, sid, [f"+{len(rows) - max_rows} more sites"], label="Not shown")

    return idx + 1


def _engineering_slide(reqs, sid, report, idx):
    """Dedicated slide for engineering work affecting this customer."""
    jira = report.get("jira", {})
    eng = jira.get("engineering", {})
    eng_open = eng.get("open", [])
    eng_closed = eng.get("recent_closed", [])
    jira_base = jira.get("base_url", "")

    if not eng_open and not eng_closed:
        return _missing_data_slide(reqs, sid, report, idx, "Jira engineering pipeline (in progress / shipped)")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Engineering Pipeline")

    open_count = eng.get("open_count", len(eng_open))
    closed_count = eng.get("closed_count", len(eng_closed))
    header = f"{eng.get('total', open_count + closed_count)} engineering tickets  ·  {open_count} open  ·  {closed_count} closed"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)

    # Open tickets table
    y = BODY_Y + 24
    max_y = BODY_BOTTOM

    if eng_open:
        open_title = f"In Progress ({open_count})"
        _box(reqs, f"{sid}_ot", sid, MARGIN, y, CONTENT_W, 16, open_title)
        _style(reqs, f"{sid}_ot", 0, len(open_title), bold=True, size=10, color=BLUE, font=FONT)
        y += 20

        avail = max((max_y - y) // 14 - 6, 3)
        for oi, t in enumerate(eng_open[:min(avail, 8)]):
            assignee = t.get("assignee") or "unassigned"
            status = t.get("status", "")[:12]
            key = t["key"]
            line = f"{key}  {status:12s}  {t['summary'][:32]}  [{assignee}]"
            _box(reqs, f"{sid}_o{oi}", sid, MARGIN, y, CONTENT_W, 14, line)
            _style(reqs, f"{sid}_o{oi}", 0, len(line), size=8, color=NAVY, font=MONO)
            ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
            _style(reqs, f"{sid}_o{oi}", 0, len(key), bold=True, size=8, color=BLUE,
                   font=MONO, link=ticket_url)
            y += 14
        y += 8

    if eng_closed and y < max_y - 40:
        closed_title = f"Recently Shipped ({closed_count})"
        _box(reqs, f"{sid}_ct", sid, MARGIN, y, CONTENT_W, 16, closed_title)
        _style(reqs, f"{sid}_ct", 0, len(closed_title), bold=True, size=10,
               color={"red": 0.13, "green": 0.55, "blue": 0.13}, font=FONT)
        y += 20

        avail = max((max_y - y) // 14, 2)
        for ci, t in enumerate(eng_closed[:min(avail, 5)]):
            key = t["key"]
            line = f"{key}  {t['summary'][:40]}  ({t.get('updated', '')})"
            _box(reqs, f"{sid}_c{ci}", sid, MARGIN, y, CONTENT_W, 14, line)
            _style(reqs, f"{sid}_c{ci}", 0, len(line), size=8, color=NAVY, font=MONO)
            ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
            _style(reqs, f"{sid}_c{ci}", 0, len(key), bold=True, size=8,
                   color={"red": 0.13, "green": 0.55, "blue": 0.13}, font=MONO,
                   link=ticket_url)
            y += 14

    return idx + 1


def _enhancement_requests_slide(reqs, sid, report, idx):
    """Customer enhancement requests from the ER project."""
    jira = report.get("jira", {})
    er = jira.get("enhancements", {})
    er_open = er.get("open", [])
    er_shipped = er.get("shipped", [])
    er_declined = er.get("declined", [])
    jira_base = jira.get("base_url", "")

    if not er_open and not er_shipped and not er_declined:
        return _missing_data_slide(reqs, sid, report, idx, "Jira enhancement requests (open / shipped / declined)")

    _slide(reqs, sid, idx)
    _bg(reqs, sid, WHITE)
    _slide_title(reqs, sid, "Enhancement Requests")

    open_n = er.get("open_count", len(er_open))
    shipped_n = er.get("shipped_count", len(er_shipped))
    declined_n = er.get("declined_count", len(er_declined))
    total = er.get("total", open_n + shipped_n + declined_n)

    header = f"{total} enhancement requests  ·  {open_n} open  ·  {shipped_n} shipped  ·  {declined_n} declined"
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 16, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=10, color=NAVY, font=FONT, bold=True)

    col_gap = 20
    left_x = MARGIN
    left_w = (CONTENT_W - col_gap) // 2
    right_x = MARGIN + left_w + col_gap
    right_w = CONTENT_W - left_w - col_gap
    body_top = BODY_Y + 24
    max_y = BODY_BOTTOM

    # ── LEFT: Open requests ──
    left_y = body_top
    if er_open:
        open_title = f"Open ({open_n})"
        _box(reqs, f"{sid}_otitle", sid, left_x, left_y, left_w, 16, open_title)
        _style(reqs, f"{sid}_otitle", 0, len(open_title), bold=True, size=10, color=BLUE, font=FONT)
        left_y += 20

        avail = max((max_y - left_y) // 28, 2)
        for oi, t in enumerate(er_open[:min(avail, 6)]):
            key = t["key"]
            prio = t.get("priority", "")
            prio_short = prio.split(":")[0] if ":" in prio else prio[:8]
            line1 = f"{key}  {prio_short}"
            line2 = t["summary"][:38]
            text = f"{line1}\n{line2}"
            _box(reqs, f"{sid}_eo{oi}", sid, left_x, left_y, left_w, 26, text)
            _style(reqs, f"{sid}_eo{oi}", 0, len(text), size=8, color=NAVY, font=FONT)
            ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
            _style(reqs, f"{sid}_eo{oi}", 0, len(key), bold=True, size=8, color=BLUE,
                   font=MONO, link=ticket_url)
            left_y += 28

    # ── RIGHT: Shipped ──
    right_y = body_top
    if er_shipped:
        ship_title = f"Shipped ({shipped_n})"
        _box(reqs, f"{sid}_stitle", sid, right_x, right_y, right_w, 16, ship_title)
        _style(reqs, f"{sid}_stitle", 0, len(ship_title), bold=True, size=10,
               color={"red": 0.13, "green": 0.55, "blue": 0.13}, font=FONT)
        right_y += 20

        avail = max((max_y - right_y) // 28, 2)
        for si, t in enumerate(er_shipped[:min(avail, 6)]):
            key = t["key"]
            line1 = f"{key}  ({t.get('updated', '')})"
            line2 = t["summary"][:38]
            text = f"{line1}\n{line2}"
            _box(reqs, f"{sid}_es{si}", sid, right_x, right_y, right_w, 26, text)
            _style(reqs, f"{sid}_es{si}", 0, len(text), size=8, color=NAVY, font=FONT)
            ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
            _style(reqs, f"{sid}_es{si}", 0, len(key), bold=True, size=8,
                   color={"red": 0.13, "green": 0.55, "blue": 0.13}, font=MONO,
                   link=ticket_url)
            right_y += 28

    if er_declined and right_y < max_y - 40:
        dec_title = f"Declined / Deferred ({declined_n})"
        _box(reqs, f"{sid}_dtitle", sid, right_x, right_y, right_w, 16, dec_title)
        _style(reqs, f"{sid}_dtitle", 0, len(dec_title), bold=True, size=10, color=GRAY, font=FONT)
        right_y += 20

        avail = max((max_y - right_y) // 14, 1)
        for di, t in enumerate(er_declined[:min(avail, 3)]):
            key = t["key"]
            line = f"{key}  {t['summary'][:36]}"
            _box(reqs, f"{sid}_ed{di}", sid, right_x, right_y, right_w, 14, line)
            _style(reqs, f"{sid}_ed{di}", 0, len(line), size=8, color=GRAY, font=MONO)
            ticket_url = f"{jira_base}/browse/{key}" if jira_base else None
            if ticket_url:
                _style(reqs, f"{sid}_ed{di}", 0, len(key), size=8, color=GRAY, font=MONO,
                       link=ticket_url)
            right_y += 14

    return idx + 1


# ── Bespoke slide builders (replicate CSM-designed slides) ──

# Colors extracted from the Safran QBR template
_BESPOKE_NAVY = {"red": 0.031, "green": 0.239, "blue": 0.471}   # #083d78 accent navy
_BESPOKE_DARK = {"red": 0.031, "green": 0.110, "blue": 0.200}   # #081c33 deep bg

def _bespoke_cover_slide(reqs, sid, report, idx):
    """Branded cover slide: customer name, deck title, date."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    customer = report.get("customer", report.get("account", {}).get("customer", ""))
    days = report.get("days", 30)
    quarter_label = report.get("quarter")
    date_str = _date_range(days, quarter_label,
                           report.get("quarter_start"), report.get("quarter_end"))
    raw_date = report.get("generated", "")
    try:
        generated = datetime.datetime.strptime(raw_date, "%Y-%m-%d").strftime("%B %-d, %Y")
    except (ValueError, TypeError):
        generated = raw_date or datetime.datetime.now().strftime("%B %-d, %Y")

    # Decorative tagline (faint, right side)
    tagline = "THE RIGHT PART.\nIN THE RIGHT PLACE.\nAT THE RIGHT TIME."
    _box(reqs, f"{sid}_tag", sid, SLIDE_W - 240, 30, 220, 120, tagline)
    _style(reqs, f"{sid}_tag", 0, len(tagline), size=11, color=_BESPOKE_NAVY, font=FONT,
           bold=True)

    # Main title — generous height so wrapping doesn't overlap the customer name
    title = "Executive business review"
    title_top = SLIDE_H * 0.22
    _box(reqs, f"{sid}_t", sid, MARGIN + 6, title_top, 560, 130, title)
    _style(reqs, f"{sid}_t", 0, len(title), size=50, color=WHITE, font=FONT_SERIF)

    # Customer name — well below the title block
    cust_top = title_top + 140
    _box(reqs, f"{sid}_c", sid, MARGIN + 6, cust_top, 500, 36, customer)
    _style(reqs, f"{sid}_c", 0, len(customer), size=24, color=MINT, font=FONT, bold=True)

    # Date
    date_text = generated
    _box(reqs, f"{sid}_d", sid, MARGIN + 6, cust_top + 42, 500, 28, date_text)
    _style(reqs, f"{sid}_d", 0, len(date_text), size=19, color=MINT, font=FONT)

    # Confidential footer
    footer = "Proprietary & Confidential"
    _box(reqs, f"{sid}_f", sid, SLIDE_W - 220, SLIDE_H - 28, 200, 16, footer)
    _style(reqs, f"{sid}_f", 0, len(footer), size=8, color=GRAY, font=FONT)

    return idx + 1


def _bespoke_agenda_slide(reqs, sid, report, idx):
    """Numbered agenda slide generated from the deck's slide plan."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    # Accent rounded rectangle on the right half
    _rect(reqs, f"{sid}_accent", sid, SLIDE_W * 0.48, 0, SLIDE_W * 0.52, SLIDE_H, _BESPOKE_NAVY)

    # Title
    _box(reqs, f"{sid}_t", sid, MARGIN, MARGIN, 300, 50, "Agenda")
    _style(reqs, f"{sid}_t", 0, len("Agenda"), size=38, color=WHITE, font=FONT_SERIF)

    # Build agenda items from the slide plan.
    # Prefer divider titles (section headings). Fall back to non-structural slide titles.
    slide_plan = report.get("_slide_plan", [])
    divider_items = [
        entry.get("title", "")
        for entry in slide_plan
        if entry.get("slide_type", entry.get("id", "")) == "bespoke_divider"
        and entry.get("title")
    ]
    if divider_items:
        items = divider_items
    else:
        skip_types = {"bespoke_cover", "bespoke_agenda", "title", "data_quality", "skip"}
        items = [
            entry.get("title", entry.get("id", "").replace("_", " ").title())
            for entry in slide_plan
            if entry.get("slide_type", entry.get("id", "")) not in skip_types
        ]

    # Render numbered list — dynamically size to fit
    x = SLIDE_W * 0.52
    y_start = MARGIN + 20
    avail_h = SLIDE_H - MARGIN * 2 - 20
    n_items = len(items)
    line_h = max(28, min(42, avail_h // max(n_items, 1)))
    font_sz = 18 if n_items > 8 else 20
    num_sz = 20 if n_items > 8 else 22
    max_items = min(n_items, avail_h // line_h)

    y = y_start
    for i, item in enumerate(items[:max_items]):
        num = f"{i + 1:02d}"
        label = item[:50] + "…" if len(item) > 50 else item
        _box(reqs, f"{sid}_n{i}", sid, x, y, 40, line_h, num)
        _style(reqs, f"{sid}_n{i}", 0, len(num), size=num_sz, color=MINT, font=FONT, bold=True)

        _box(reqs, f"{sid}_i{i}", sid, x + 48, y, 280, line_h, label)
        _style(reqs, f"{sid}_i{i}", 0, len(label), size=font_sz, color=WHITE, font=FONT)
        y += line_h

    return idx + 1


def _bespoke_divider_slide(reqs, sid, report, idx):
    """Section divider slide with LeanDNA tagline and section title."""
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    # Read section title from the current slide definition
    entry = report.get("_current_slide", {})
    section_title = entry.get("title", entry.get("note", ""))

    # Stacked tagline (left side, large)
    lines = [
        ("THE RIGHT PART.", 28, True),
        ("In the right place.", 28, False),
        ("AT THE RIGHT TIME.", 26, False),
    ]
    ty = SLIDE_H * 0.18
    for li, (text, size, bold) in enumerate(lines):
        _box(reqs, f"{sid}_tl{li}", sid, MARGIN, ty, 400, 36, text)
        _style(reqs, f"{sid}_tl{li}", 0, len(text), size=size, color=WHITE, font=FONT, bold=bold)
        ty += 40

    # Section title (prominent, centered-lower)
    if section_title:
        _box(reqs, f"{sid}_sec", sid, MARGIN, SLIDE_H * 0.65, CONTENT_W, 50, section_title)
        _style(reqs, f"{sid}_sec", 0, len(section_title), size=32, color=MINT, font=FONT_SERIF)

    # Confidential footer
    footer = "Proprietary & Confidential"
    _box(reqs, f"{sid}_f", sid, SLIDE_W - 220, SLIDE_H - 28, 200, 16, footer)
    _style(reqs, f"{sid}_f", 0, len(footer), size=8, color=GRAY, font=FONT)

    return idx + 1


def _bespoke_deployment_slide(reqs, sid, report, idx):
    """Deployment overview: site count and status table from Pendo data."""
    all_sites = report.get("sites", [])
    if not all_sites:
        return _missing_data_slide(reqs, sid, report, idx, "Pendo site list for deployment summary")

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Deployment — Number of Sites")

    customer = report.get("customer", report.get("account", {}).get("customer", ""))
    raw_gen = report.get("generated", "")
    try:
        generated = datetime.datetime.strptime(raw_gen, "%Y-%m-%d").strftime("%B %-d, %Y")
    except (ValueError, TypeError):
        generated = raw_gen or datetime.datetime.now().strftime("%B %-d, %Y")
    subtitle = f"As of {generated}"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y - 10, CONTENT_W, 18, subtitle)
    _style(reqs, f"{sid}_sub", 0, len(subtitle), size=10, color=GRAY, font=FONT)

    # Health status from CS Report if available
    cs_health = report.get("cs_platform_health", {})
    site_health = {}
    for row in cs_health.get("sites", []):
        name = row.get("site", "")
        status = row.get("health_status", "")
        if name and status:
            site_health[name.lower()] = status

    customer_prefix = customer.strip()

    def _short_site(name: str) -> str:
        n = name
        if customer_prefix and n.lower().startswith(customer_prefix.lower()):
            n = n[len(customer_prefix):].lstrip(" -·")
        return n[:25] if len(n) > 25 else n

    headers = ["Site", "Users", "Status", "Last Active"]
    col_widths = [220, 60, 80, 130]
    ROW_H = 26

    max_rows = (BODY_BOTTOM - (BODY_Y + 14)) // ROW_H - 1
    sites_to_show = all_sites[:max_rows]

    rows_data = []
    for s in sites_to_show:
        site_name = _short_site(s.get("sitename", "?"))
        visitors = str(s.get("visitors", 0))
        health = site_health.get(s.get("sitename", "").lower(), "—")
        last_active_raw = s.get("last_active", "—")
        try:
            last_active = datetime.datetime.strptime(
                str(last_active_raw)[:10], "%Y-%m-%d"
            ).strftime("%b %-d, %Y")
        except (ValueError, TypeError):
            last_active = str(last_active_raw)[:10] if last_active_raw else "—"
        rows_data.append([site_name, visitors, health, last_active])

    _simple_table(reqs, f"{sid}_tbl", sid, MARGIN, BODY_Y + 14,
                  col_widths, ROW_H, headers, rows_data)

    # Color-code status cells
    status_colors = {
        "GREEN": {"red": 0.1, "green": 0.6, "blue": 0.2},
        "YELLOW": {"red": 0.9, "green": 0.7, "blue": 0.1},
        "RED": {"red": 0.85, "green": 0.15, "blue": 0.15},
    }
    for ri, row in enumerate(rows_data):
        status = row[2].upper() if len(row) > 2 else ""
        if status in status_colors:
            _table_cell_bg(reqs, f"{sid}_tbl", ri + 1, 2, status_colors[status])

    if len(all_sites) > max_rows:
        omit = f"+ {len(all_sites) - max_rows} more sites not shown"
        _box(reqs, f"{sid}_omit", sid, MARGIN, BODY_BOTTOM - 14, CONTENT_W, 14, omit)
        _style(reqs, f"{sid}_omit", 0, len(omit), size=8, color=GRAY, font=FONT)

    return idx + 1


# ── Composable API (agent builds deck slide by slide) ──

# Maps slide type names to builder functions and the report keys they require
_SLIDE_BUILDERS = {
    "title": _title_slide,
    "health": _health_slide,
    "engagement": _engagement_slide,
    "sites": _sites_slide,
    "features": _features_slide,
    "champions": _champions_slide,
    "benchmarks": _benchmarks_slide,
    "exports": _exports_slide,
    "depth": _depth_slide,
    "kei": _kei_slide,
    "guides": _guides_slide,
    "jira": _jira_slide,
    "custom": _custom_slide,
    "signals": _signals_slide,
    "platform_health": _platform_health_slide,
    "supply_chain": _supply_chain_slide,
    "platform_value": _platform_value_slide,
    "data_quality": _data_quality_slide,
    "portfolio_title": _portfolio_title_slide,
    "portfolio_signals": _portfolio_signals_slide,
    "portfolio_trends": _portfolio_trends_slide,
    "portfolio_leaders": _portfolio_leaders_slide,
    "team": _team_slide,
    "sla_health": _sla_health_slide,
    "cross_validation": _cross_validation_slide,
    "engineering": _engineering_slide,
    "enhancements": _enhancement_requests_slide,
    "bespoke_cover": _bespoke_cover_slide,
    "bespoke_agenda": _bespoke_agenda_slide,
    "bespoke_divider": _bespoke_divider_slide,
    "bespoke_deployment": _bespoke_deployment_slide,
}

# Which report keys each slide type needs (so the agent knows what data to supply)
SLIDE_DATA_REQUIREMENTS = {
    "title": ["customer", "days", "generated", "account"],
    "health": ["engagement", "benchmarks", "account"],
    "engagement": ["engagement", "account"],
    "sites": ["sites"],
    "features": ["top_pages", "top_features"],
    "champions": ["champions", "at_risk_users"],
    "benchmarks": ["benchmarks", "account"],
    "exports": ["exports"],
    "depth": ["depth"],
    "kei": ["kei"],
    "guides": ["guides"],
    "jira": ["jira"],
    "custom": ["title", "sections"],
    "signals": ["signals"],
    "platform_health": ["cs_platform_health"],
    "supply_chain": ["cs_supply_chain"],
    "platform_value": ["cs_platform_value"],
    "sla_health": ["jira"],
    "cross_validation": ["cs_platform_health", "sites", "engagement"],
    "engineering": ["jira"],
    "enhancements": ["jira"],
    "data_quality": [],
    "portfolio_title": ["customer_count", "days", "generated"],
    "portfolio_signals": ["portfolio_signals"],
    "portfolio_trends": ["portfolio_trends"],
    "portfolio_leaders": ["portfolio_leaders"],
    "team": ["customer"],
    "bespoke_cover": ["customer", "days"],
    "bespoke_agenda": [],
    "bespoke_divider": [],
    "bespoke_deployment": ["sites"],
}


_output_folder_cache: tuple[str, str] | None = None  # (date_str, folder_id)


def _get_deck_output_folder() -> str | None:
    """Return the ID of today's date-stamped subfolder (e.g. Decks-2026-03-06), creating it if needed."""
    global _output_folder_cache
    if not GOOGLE_DRIVE_FOLDER_ID:
        return None
    today = datetime.date.today().isoformat()
    if _output_folder_cache and _output_folder_cache[0] == today:
        return _output_folder_cache[1]
    from .drive_config import _find_or_create_folder
    folder_id = _find_or_create_folder(f"Decks-{today}", GOOGLE_DRIVE_FOLDER_ID)
    _output_folder_cache = (today, folder_id)
    return folder_id


def create_empty_deck(customer: str, days: int = 30, deck_name: str | None = None) -> dict[str, Any]:
    """Create an empty presentation. Returns {deck_id, url} for use with add_slide."""
    try:
        slides_service, drive_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    label = deck_name or "Usage Health Review"
    title = f"{customer} — {label} ({_date_range(days)})"
    try:
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            file_meta["parents"] = [output_folder]
        f = drive_service.files().create(body=file_meta).execute()
        deck_id = f["id"]
        logger.info("Created deck %s: %s", deck_id, title)
    except HttpError as e:
        return {"error": str(e)}

    # Delete the default blank slide
    try:
        pres = slides_service.presentations().get(presentationId=deck_id).execute()
        default_id = pres["slides"][0]["objectId"]
        slides_service.presentations().batchUpdate(
            presentationId=deck_id,
            body={"requests": [{"deleteObject": {"objectId": default_id}}]},
        ).execute()
    except Exception:
        pass

    return {
        "deck_id": deck_id,
        "url": f"https://docs.google.com/presentation/d/{deck_id}/edit",
    }


_slide_counter: dict[str, int] = {}


def add_slide(deck_id: str, slide_type: str, data: dict[str, Any]) -> dict[str, Any]:
    """Add one slide to an existing deck.

    Args:
        deck_id: Presentation ID from create_empty_deck.
        slide_type: One of: title, health, engagement, sites, features, champions, benchmarks, exports, depth, kei, guides, custom, signals.
        data: Dict with the keys required for that slide type (see SLIDE_DATA_REQUIREMENTS).

    Returns:
        {slide_type, status} or {error}.
    """
    builder = _SLIDE_BUILDERS.get(slide_type)
    if not builder:
        return {"error": f"Unknown slide type '{slide_type}'. Valid: {', '.join(_SLIDE_BUILDERS)}"}

    try:
        slides_service, _ = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    # Use local counter as insertion index to avoid an API round-trip per slide
    count = _slide_counter.get(deck_id, 0)
    _slide_counter[deck_id] = count + 1
    idx = count
    sid = f"s_{slide_type}_{count}"

    reqs: list[dict] = []
    try:
        new_idx = builder(reqs, sid, data, idx)
    except (KeyError, TypeError, IndexError) as e:
        required = SLIDE_DATA_REQUIREMENTS.get(slide_type, [])
        return {
            "error": f"Slide '{slide_type}' data is missing required key: {e}. Required keys: {required}",
            "slide_type": slide_type,
        }

    if not reqs:
        return {"slide_type": slide_type, "status": "skipped (no data)"}

    try:
        slides_service.presentations().batchUpdate(
            presentationId=deck_id, body={"requests": reqs},
        ).execute()
    except HttpError as e:
        return {"error": str(e), "slide_type": slide_type}

    return {"slide_type": slide_type, "status": "added", "position": idx + 1}


# ── Monolith deck creation (deck-definition-driven) ──

def create_health_deck(
    report: dict[str, Any],
    deck_id: str = "cs_health_review",
    thumbnails: bool = True,
) -> dict[str, Any]:
    """Create a deck from a customer health report using a deck definition.

    Args:
        report: Full customer health report from PendoClient.get_customer_health_report().
        deck_id: Which deck definition to use. Defaults to 'cs_health_review'.
        thumbnails: Whether to export slide thumbnails. Disable for batch runs.
    """
    if "error" in report:
        return {"error": report["error"]}

    is_portfolio = report.get("type") == "portfolio"
    customer = report.get("customer", "Portfolio") if not is_portfolio else "Portfolio"
    days = report.get("days", 30)
    quarter_label = report.get("quarter")

    from .qa import qa
    qa.begin(customer)

    try:
        slides_service, drive_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    from .deck_loader import resolve_deck

    resolved = resolve_deck(deck_id, customer)
    deck_name = resolved.get("name", "Health Review")
    date_str = _date_range(days, quarter_label, report.get("quarter_start"), report.get("quarter_end"))
    if is_portfolio:
        title = f"{deck_name} ({date_str})"
    else:
        title = f"{customer} — {deck_name} ({date_str})"

    try:
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            file_meta["parents"] = [output_folder]
        file = drive_service.files().create(body=file_meta).execute()
        pres_id = file["id"]
        logger.info("Created presentation %s: %s", pres_id, title)
    except HttpError as e:
        err_str = str(e)
        if "rate" in err_str.lower() or "quota" in err_str.lower():
            return {"error": f"Rate limit: {err_str}. Wait and retry."}
        return {"error": err_str}

    # Provide a DeckCharts instance for builders that want to embed Sheets charts
    try:
        from .charts import DeckCharts
        report["_charts"] = DeckCharts(title)
    except Exception as e:
        logger.debug("Charts unavailable (will skip chart embeds): %s", e)

    slide_plan = resolved.get("slides", [])
    reqs: list[dict] = []
    idx = 1

    report["_slide_plan"] = slide_plan

    for entry in slide_plan:
        slide_type = entry.get("slide_type", entry["id"])
        builder = _SLIDE_BUILDERS.get(slide_type)
        if builder:
            report["_current_slide"] = entry
            sid = f"s_{entry['id']}_{idx}"
            idx = builder(reqs, sid, report, idx)

    slides_created = idx - 1

    try:
        pres = slides_service.presentations().get(presentationId=pres_id).execute()
        default_id = pres["slides"][0]["objectId"]
        reqs.append({"deleteObject": {"objectId": default_id}})
    except Exception:
        pass

    try:
        slides_service.presentations().batchUpdate(
            presentationId=pres_id, body={"requests": reqs},
        ).execute()
    except HttpError as e:
        logger.exception("Failed to build slides")
        return {"error": str(e), "presentation_id": pres_id}

    result = {
        "presentation_id": pres_id,
        "url": f"https://docs.google.com/presentation/d/{pres_id}/edit",
        "customer": customer,
        "slides_created": slides_created,
    }

    if thumbnails:
        try:
            thumbs = export_slide_thumbnails(pres_id)
            result["thumbnails"] = [str(p) for p in thumbs]
            logger.info("Saved %d slide thumbnails for %s", len(thumbs), customer)
        except Exception as e:
            logger.warning("Thumbnail export failed: %s", e)

    return result


def create_portfolio_deck(
    days: int = 30,
    max_customers: int | None = None,
    quarter: "QuarterRange | None" = None,
) -> dict[str, Any]:
    """Generate a single portfolio-level deck across all customers."""
    from .pendo_client import PendoClient

    client = PendoClient()
    report = client.get_portfolio_report(days=days, max_customers=max_customers)
    if quarter:
        report["quarter"] = quarter.label
        report["quarter_start"] = quarter.start.isoformat()
        report["quarter_end"] = quarter.end.isoformat()
    return create_health_deck(report, deck_id="portfolio_review")


def create_health_decks_for_customers(
    customer_names: list[str],
    days: int = 30,
    max_customers: int | None = None,
    deck_id: str = "cs_health_review",
    workers: int = 4,
    thumbnails: bool = False,
    quarter: "QuarterRange | None" = None,
) -> list[dict[str, Any]]:
    """Create one deck per customer using a deck definition (parallel).

    Args:
        customer_names: List of customer names to generate decks for.
        days: Lookback window in days.
        max_customers: Cap on how many to generate.
        deck_id: Which deck definition to use (default: cs_health_review).
        workers: Concurrent deck-creation threads (default 4).
        thumbnails: Export slide thumbnails (default False for batch — saves API quota).
        quarter: Optional QuarterRange to label slides with quarter info.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .pendo_client import PendoClient

    client = PendoClient()
    client.preload(days)
    customers = customer_names[:max_customers] if max_customers else customer_names
    quarter_label = quarter.label if quarter else None
    quarter_start = quarter.start.isoformat() if quarter else None
    quarter_end = quarter.end.isoformat() if quarter else None

    def _build_one(idx_name: tuple[int, str]) -> dict[str, Any]:
        i, name = idx_name
        logger.info("Generating deck %d/%d: %s (%s)", i + 1, len(customers), name, deck_id)
        try:
            report = client.get_customer_health_report(name, days=days)
            if quarter_label:
                report["quarter"] = quarter_label
                report["quarter_start"] = quarter_start
                report["quarter_end"] = quarter_end
            return create_health_deck(report, deck_id=deck_id, thumbnails=thumbnails)
        except Exception as e:
            return {"error": str(e), "customer": name}

    results: list[dict[str, Any]] = [{}] * len(customers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_build_one, (i, n)): i for i, n in enumerate(customers)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception as e:
                results[idx] = {"error": str(e), "customer": customers[idx]}
            r = results[idx]
            if "error" in r and "403" in str(r.get("error", "")):
                logger.error("Got 403 for %s — cancelling remaining.", customers[idx])
                for f in futures:
                    f.cancel()
                break

    return results


# ── Legacy (backward compat) ──

def create_deck_for_customer(customer, sites, days=30):
    if not sites:
        return {"error": f"No sites for '{customer}'"}
    try:
        slides_service, drive_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}
    title = f"{customer} - Usage Report ({_date_range(days)})"
    try:
        meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        output_folder = _get_deck_output_folder()
        if output_folder:
            meta["parents"] = [output_folder]
        f = drive_service.files().create(body=meta).execute()
        pid = f["id"]
    except HttpError as e:
        return {"error": str(e)}
    r = []
    ix = 1
    for i, s in enumerate(sites):
        sid = f"ls_{i}"
        r.append({"createSlide": {"objectId": sid, "insertionIndex": ix}}); ix += 1
        _box(r, f"lt_{i}", sid, 60, 40, 600, 50, s.get("sitename", "?"))
        body = f"Page views: {s.get('page_views',0)}\nFeature clicks: {s.get('feature_clicks',0)}\nEvents: {s.get('total_events',0)}\nMinutes: {s.get('total_minutes',0)}"
        _box(r, f"lb_{i}", sid, 60, 100, 600, 280, body)
    try:
        slides_service.presentations().batchUpdate(presentationId=pid, body={"requests": r}).execute()
    except HttpError as e:
        return {"error": str(e), "presentation_id": pid}
    return {"presentation_id": pid, "url": f"https://docs.google.com/presentation/d/{pid}/edit", "customer": customer, "slides_created": len(sites)}


def create_decks_for_all_customers(by_customer, customer_list, days=30, delay_seconds=2.0, max_customers=None):
    cs = customer_list[:max_customers] if max_customers else customer_list
    results = []
    for i, c in enumerate(cs):
        if i > 0:
            time.sleep(delay_seconds)
        results.append(create_deck_for_customer(c, by_customer.get(c, []), days))
        if "error" in results[-1] and "403" in str(results[-1].get("error", "")):
            results.append({"error": "Stopped: 403.", "customers_attempted": i + 1}); break
    return results


# ── Slide thumbnail export ──

def export_slide_thumbnails(
    presentation_id: str,
    output_dir: str | Path | None = None,
    size: str = "LARGE",
) -> list[Path]:
    """Download PNG thumbnails for every slide in a presentation.

    Args:
        presentation_id: Google Slides presentation ID or full URL.
        output_dir: Where to save PNGs. Defaults to a temp directory.
        size: Thumbnail size — "SMALL" (default 200px) or "LARGE" (default 800px).

    Returns:
        List of saved PNG file paths.
    """
    import re
    import tempfile
    import urllib.request

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", presentation_id)
    pres_id = match.group(1) if match else presentation_id

    slides_service, _ = _get_service()
    pres = slides_service.presentations().get(presentationId=pres_id).execute()
    title = pres.get("title", pres_id)
    slides = pres.get("slides", [])

    if not slides:
        logger.warning("Presentation %s has no slides", pres_id)
        return []

    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix=f"bpo-thumbs-{pres_id[:12]}-"))
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []
    for i, slide in enumerate(slides):
        page_id = slide["objectId"]
        thumb = slides_service.presentations().pages().getThumbnail(
            presentationId=pres_id,
            pageObjectId=page_id,
            thumbnailProperties_thumbnailSize=size,
        ).execute()
        url = thumb["contentUrl"]
        dest = out / f"slide_{i + 1:02d}.png"
        urllib.request.urlretrieve(url, str(dest))
        saved.append(dest)

    logger.info("Exported %d thumbnails for '%s' → %s", len(saved), title, out)
    return saved
