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


def _date_range(days: int) -> str:
    """Format a human-readable date range like 'Feb 3 – Mar 5, 2026'."""
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days)
    if start.year == end.year:
        return f"{start.strftime('%b %-d')} – {end.strftime('%b %-d, %Y')}"
    return f"{start.strftime('%b %-d, %Y')} – {end.strftime('%b %-d, %Y')}"


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


def _style(reqs, oid, start, end, bold=False, size=None, color=None, font=None, italic=False):
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
    sub = f"Product Usage Review  ·  {_date_range(report['days'])}"
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

    # Horizontal bar chart with drawn rectangles
    tiers = [
        ("Active (7d)", eng["active_7d"]),
        ("Active (8–30d)", eng["active_30d"]),
        ("Dormant (30d+)", eng["dormant"]),
    ]
    bar_x = MARGIN
    bar_max_w = 280
    row_h = 48
    y = BODY_Y + 4

    for i, (label, count) in enumerate(tiers):
        pct = round(count / max(total, 1) * 100)
        bar_w = max(4, pct / 100 * bar_max_w)

        # Label + count
        txt = f"{label}   {count}  ({pct}%)"
        _box(reqs, f"{sid}_lbl{i}", sid, bar_x, y, 320, 18, txt)
        _style(reqs, f"{sid}_lbl{i}", 0, len(txt), size=11, color=NAVY, font=FONT)
        _style(reqs, f"{sid}_lbl{i}", 0, len(label), bold=True)

        # Bar
        _rect(reqs, f"{sid}_bar{i}", sid, bar_x, y + 20, bar_w, 12, NAVY if i < 2 else GRAY)

        y += row_h

    # Role breakdown (right column)
    active_roles = list(eng["role_active"].items())[:6]
    dormant_roles = list(eng["role_dormant"].items())[:6]

    role_lines = ["Active Roles"]
    for r, c in active_roles:
        role_lines.append(f"  {r}: {c}")
    role_lines.append("")
    role_lines.append("Dormant Roles")
    for r, c in dormant_roles:
        role_lines.append(f"  {r}: {c}")
    role_text = "\n".join(role_lines)

    _box(reqs, f"{sid}_roles", sid, 400, BODY_Y, 280, 290, role_text)
    _style(reqs, f"{sid}_roles", 0, len(role_text), size=10, color=NAVY, font=FONT)

    ah = "Active Roles"
    _style(reqs, f"{sid}_roles", 0, len(ah), bold=True, size=11, color=BLUE)
    dh = "Dormant Roles"
    di = role_text.index(dh)
    _style(reqs, f"{sid}_roles", di, di + len(dh), bold=True, size=11, color=BLUE)

    return idx + 1


def _sites_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Site Comparison")

    all_sites = report["sites"]
    if not all_sites:
        _box(reqs, f"{sid}_e", sid, MARGIN, BODY_Y, CONTENT_W, 30, "No site data available")
        _style(reqs, f"{sid}_e", 0, 22, size=12, color=GRAY, font=FONT, italic=True)
        return idx + 1

    customer_prefix = report.get("account", {}).get("customer", "").strip()

    def _short_site(name: str) -> str:
        n = name
        if customer_prefix and n.lower().startswith(customer_prefix.lower()):
            n = n[len(customer_prefix):].lstrip(" -·")
        return n[:22] if len(n) > 22 else n

    headers = ["Site", "Users", "Pages", "Features", "Events", "Minutes", "Last Active"]
    col_widths = [180, 50, 55, 65, 60, 60, 80]
    ROW_H = 28

    show_total = len(all_sites) > 1
    max_data_rows = (BODY_BOTTOM - BODY_Y) // ROW_H - 1 - (1 if show_total else 0)

    omitted_sites: list[str] = []
    if len(all_sites) > max_data_rows:
        sites = all_sites[:max_data_rows - 1]
        others = all_sites[max_data_rows - 1:]
        omitted_sites = [s["sitename"] for s in others]
        others_row = {
            "sitename": f"Others ({len(others)} sites)",
            "visitors": sum(s["visitors"] for s in others),
            "page_views": sum(s["page_views"] for s in others),
            "feature_clicks": sum(s["feature_clicks"] for s in others),
            "total_events": sum(s["total_events"] for s in others),
            "total_minutes": sum(s["total_minutes"] for s in others),
            "last_active": "",
        }
        sites.append(others_row)
    else:
        sites = all_sites

    num_rows = 1 + len(sites) + (1 if show_total else 0)
    num_cols = len(headers)
    table_id = f"{sid}_table"

    tbl_w = sum(col_widths)
    tbl_h = num_rows * ROW_H
    reqs.append({
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": sid,
                "size": _sz(tbl_w, tbl_h),
                "transform": _tf(MARGIN, BODY_Y),
            },
            "rows": num_rows,
            "columns": num_cols,
        }
    })

    def _cell_loc(row, col):
        return {"rowIndex": row, "columnIndex": col}

    def _cell_text(row, col, text):
        reqs.append({"insertText": {"objectId": table_id,
                     "cellLocation": _cell_loc(row, col),
                     "text": text, "insertionIndex": 0}})

    def _cell_style(row, col, text_len, bold=False, color=None, size=9, font=FONT, align=None):
        if text_len > 0:
            s: dict[str, Any] = {"fontSize": {"magnitude": size, "unit": "PT"}}
            f = ["fontSize"]
            if bold:
                s["bold"] = True; f.append("bold")
            if color:
                s["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}; f.append("foregroundColor")
            if font:
                s["fontFamily"] = font; f.append("fontFamily")
            reqs.append({
                "updateTextStyle": {
                    "objectId": table_id, "cellLocation": _cell_loc(row, col),
                    "textRange": {"type": "FIXED_RANGE", "startIndex": 0, "endIndex": text_len},
                    "style": s, "fields": ",".join(f),
                }
            })
        if align:
            reqs.append({
                "updateParagraphStyle": {
                    "objectId": table_id, "cellLocation": _cell_loc(row, col),
                    "textRange": {"type": "ALL"},
                    "style": {"alignment": align},
                    "fields": "alignment",
                }
            })

    def _cell_bg(row, col, color):
        reqs.append({
            "updateTableCellProperties": {
                "objectId": table_id,
                "tableRange": {"location": {"rowIndex": row, "columnIndex": col}, "rowSpan": 1, "columnSpan": 1},
                "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}},
                "fields": "tableCellBackgroundFill",
            }
        })

    _clean_table(reqs, table_id, num_rows, num_cols)

    for ci, h in enumerate(headers):
        _cell_text(0, ci, h)
        _cell_style(0, ci, len(h), bold=True, color=GRAY, size=8, font=FONT,
                     align="END" if 1 <= ci <= 5 else None)
        _cell_bg(0, ci, WHITE)

    for ri, s in enumerate(sites):
        row = ri + 1
        vals = [
            _short_site(s["sitename"]),
            f'{s["visitors"]:,}',
            f'{s["page_views"]:,}',
            f'{s["feature_clicks"]:,}',
            f'{s["total_events"]:,}',
            f'{s["total_minutes"]:,}',
            s["last_active"],
        ]
        for ci, v in enumerate(vals):
            _cell_text(row, ci, v)
            _cell_style(row, ci, len(v), color=NAVY, size=8, font=FONT,
                         align="END" if 1 <= ci <= 5 else None)
            _cell_bg(row, ci, WHITE)

    if show_total:
        total_row_idx = len(sites) + 1
        reqs.append({
            "updateTableBorderProperties": {
                "objectId": table_id,
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
            f'{sum(s["visitors"] for s in all_sites):,}',
            f'{sum(s["page_views"] for s in all_sites):,}',
            f'{sum(s["feature_clicks"] for s in all_sites):,}',
            f'{sum(s["total_events"] for s in all_sites):,}',
            f'{sum(s["total_minutes"] for s in all_sites):,}',
            "",
        ]
        for ci, v in enumerate(totals):
            if v:
                _cell_text(total_row_idx, ci, v)
                _cell_style(total_row_idx, ci, len(v), bold=True, color=NAVY, size=8, font=FONT,
                             align="END" if 1 <= ci <= 5 else None)
            _cell_bg(total_row_idx, ci, WHITE)

    _omission_note(reqs, sid, omitted_sites, label="Rolled into Others")

    return idx + 1


def _features_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Feature Adoption")

    pages = report["top_pages"]
    features = report["top_features"]

    # Pages column
    pl = ["Top Pages"]
    for i, p in enumerate(pages[:7], 1):
        nm = p["name"][:36] if len(p["name"]) > 36 else p["name"]
        pl.append(f"  {i}. {nm}  ({p['events']:,} events)")
    if not pages:
        pl.append("  No data")
    pt = "\n".join(pl)
    _box(reqs, f"{sid}_pg", sid, MARGIN, BODY_Y, 310, 290, pt)
    _style(reqs, f"{sid}_pg", 0, len(pt), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_pg", 0, len("Top Pages"), bold=True, size=11, color=BLUE)

    # Features column
    fl = ["Top Features"]
    for i, f in enumerate(features[:7], 1):
        nm = f["name"][:36] if len(f["name"]) > 36 else f["name"]
        fl.append(f"  {i}. {nm}  ({f['events']:,} clicks)")
    if not features:
        fl.append("  No data")
    ft = "\n".join(fl)
    _box(reqs, f"{sid}_ft", sid, 380, BODY_Y, 300, 290, ft)
    _style(reqs, f"{sid}_ft", 0, len(ft), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_ft", 0, len("Top Features"), bold=True, size=11, color=BLUE)

    return idx + 1


def _champions_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Champions & At-Risk Users")

    max_per_col = (BODY_BOTTOM - BODY_Y) // 28 - 2  # ~28pt per user; reserve note row
    all_champions = report["champions"]
    all_at_risk = report["at_risk_users"]
    champions = all_champions[:max_per_col]
    at_risk_show = all_at_risk[:max_per_col]

    # Champions
    cl = ["Champions"]
    for u in champions:
        email = u["email"] or "unknown"
        if len(email) > 32:
            email = email[:29] + "..."
        cl.append(f"  {email}")
        cl.append(f"    {u['role']}  ·  last seen {u['last_visit']}")
    if not champions:
        cl.append("  No active users")
    ct = "\n".join(cl)

    _box(reqs, f"{sid}_ch", sid, MARGIN, BODY_Y, 310, 290, ct)
    _style(reqs, f"{sid}_ch", 0, len(ct), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_ch", 0, len("Champions"), bold=True, size=11, color=BLUE)

    # At-risk
    rl = ["At Risk  (30+ days inactive)"]
    for u in at_risk_show:
        email = u["email"] or "unknown"
        if len(email) > 32:
            email = email[:29] + "..."
        d = f"{int(u['days_inactive'])}d ago" if u["days_inactive"] < 999 else "never"
        rl.append(f"  {email}")
        rl.append(f"    {u['role']}  ·  {d}")
    if not all_at_risk:
        rl.append("  All users active!")
    rt = "\n".join(rl)

    _box(reqs, f"{sid}_ri", sid, 380, BODY_Y, 300, 290, rt)
    _style(reqs, f"{sid}_ri", 0, len(rt), size=10, color=NAVY, font=FONT)
    hdr = "At Risk  (30+ days inactive)"
    _style(reqs, f"{sid}_ri", 0, len(hdr), bold=True, size=11, color=BLUE)

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
        return idx

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
        return idx

    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Behavioral Depth")

    write_ratio = depth.get("write_ratio", 0)
    total = depth.get("total_feature_events", 0)
    active = depth.get("active_users", 0)
    header = (f"{total:,} feature interactions  ·  {active} active users  ·  "
              f"{write_ratio}% write ratio")
    _box(reqs, f"{sid}_hdr", sid, MARGIN, BODY_Y, CONTENT_W, 18, header)
    _style(reqs, f"{sid}_hdr", 0, len(header), size=10, color=GRAY, font=FONT)

    # Stacked horizontal bars for top categories
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

    # Read/Write/Collab summary at bottom right
    read_e = depth.get("read_events", 0)
    write_e = depth.get("write_events", 0)
    collab_e = depth.get("collab_events", 0)
    summary = f"Read: {read_e:,}\nWrite: {write_e:,}\nCollab: {collab_e:,}"
    _box(reqs, f"{sid}_rw", sid, 560, BODY_Y + 28, 100, 60, summary)
    _style(reqs, f"{sid}_rw", 0, len(summary), size=9, color=NAVY, font=MONO)
    _style(reqs, f"{sid}_rw", 0, len("Read:"), bold=True, color=BLUE)

    return idx + 1


def _kei_slide(reqs, sid, report, idx):
    kei = report.get("kei", report)
    total_q = kei.get("total_queries", 0)
    if total_q == 0 and not kei.get("users"):
        return idx

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
        return idx

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
        return idx

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
        return idx

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
        return idx

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
    title = "Book of Business Review"
    sub = f"{n} customers  ·  {_date_range(days)}"

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
        return idx

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
        return idx

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
        return idx

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
    snap = qa.summary()

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Data Quality")

    total_checks = snap["total_checks"]
    total_flags = snap["total_flags"]
    n_errors = snap["errors"]
    n_warnings = snap["warnings"]

    if total_flags == 0:
        status = f"\u2705  All checks passed ({total_checks} validations)"
        status_color = _GREEN
    elif n_errors > 0:
        status = f"\u2716  {n_errors} error{'s' if n_errors != 1 else ''}, {n_warnings} warning{'s' if n_warnings != 1 else ''} across {total_checks} checks"
        status_color = _RED
    else:
        status = f"\u26a0  {n_warnings} warning{'s' if n_warnings != 1 else ''} across {total_checks} checks"
        status_color = _AMBER

    _box(reqs, f"{sid}_st", sid, MARGIN, BODY_Y, CONTENT_W, 24, status)
    _style(reqs, f"{sid}_st", 0, len(status), bold=True, size=14, color=status_color, font=FONT)

    if total_flags == 0:
        sub = "No discrepancies found where data sources overlap (Pendo, Jira, CS Report)."
        _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y + 32, CONTENT_W, 20, sub)
        _style(reqs, f"{sid}_sub", 0, len(sub), size=10, color=GRAY, font=FONT)

        detail = ("Checks include: engagement bucket sums, active-rate calculations, "
                  "site-count consistency across sources, JIRA breakdown totals, "
                  "factory name matching, and cohort classification. "
                  "Single-source metrics (feature adoption, exports, guides, dollar values) "
                  "are not independently verified.")
        _box(reqs, f"{sid}_det", sid, MARGIN, BODY_Y + 56, CONTENT_W, 60, detail)
        _style(reqs, f"{sid}_det", 0, len(detail), size=8, color=GRAY, font=FONT, italic=True)
        return idx + 1

    y = BODY_Y + 36
    max_rows = 14
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
        if f["auto_corrected"]:
            detail_parts.append("auto-corrected")
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
        return idx

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
        return idx

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
        return idx

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
    "data_quality": [],
    "portfolio_title": ["customer_count", "days", "generated"],
    "portfolio_signals": ["portfolio_signals"],
    "portfolio_trends": ["portfolio_trends"],
    "portfolio_leaders": ["portfolio_leaders"],
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
) -> dict[str, Any]:
    """Create a deck from a customer health report using a deck definition.

    Args:
        report: Full customer health report from PendoClient.get_customer_health_report().
        deck_id: Which deck definition to use. Defaults to 'cs_health_review'.
    """
    if "error" in report:
        return {"error": report["error"]}

    is_portfolio = report.get("type") == "portfolio"
    customer = report.get("customer", "Portfolio") if not is_portfolio else "Portfolio"
    days = report.get("days", 30)

    from .qa import qa
    qa.begin(customer)

    try:
        slides_service, drive_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    from .deck_loader import resolve_deck

    resolved = resolve_deck(deck_id, customer)
    deck_name = resolved.get("name", "Health Review")
    if is_portfolio:
        title = f"{deck_name} ({_date_range(days)})"
    else:
        title = f"{customer} — {deck_name} ({_date_range(days)})"

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

    slide_plan = resolved.get("slides", [])
    reqs: list[dict] = []
    idx = 1

    for entry in slide_plan:
        slide_type = entry.get("slide_type", entry["id"])
        builder = _SLIDE_BUILDERS.get(slide_type)
        if builder:
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
) -> dict[str, Any]:
    """Generate a single portfolio-level deck across all customers."""
    from .pendo_client import PendoClient

    client = PendoClient()
    report = client.get_portfolio_report(days=days, max_customers=max_customers)
    return create_health_deck(report, deck_id="portfolio_review")


def create_health_decks_for_customers(
    customer_names: list[str],
    days: int = 30,
    max_customers: int | None = None,
    deck_id: str = "cs_health_review",
    workers: int = 4,
) -> list[dict[str, Any]]:
    """Create one deck per customer using a deck definition (parallel).

    Args:
        customer_names: List of customer names to generate decks for.
        days: Lookback window in days.
        max_customers: Cap on how many to generate.
        deck_id: Which deck definition to use (default: cs_health_review).
        workers: Concurrent deck-creation threads (default 4).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .pendo_client import PendoClient

    client = PendoClient()
    client.preload(days)
    customers = customer_names[:max_customers] if max_customers else customer_names

    def _build_one(idx_name: tuple[int, str]) -> dict[str, Any]:
        i, name = idx_name
        logger.info("Generating deck %d/%d: %s (%s)", i + 1, len(customers), name, deck_id)
        try:
            report = client.get_customer_health_report(name, days=days)
            return create_health_deck(report, deck_id=deck_id)
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
