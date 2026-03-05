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


def _slide_title(reqs, sid, text):
    """Standard content-slide title: navy text + teal underline."""
    oid = f"{sid}_ttl"
    _box(reqs, oid, sid, MARGIN, TITLE_Y, CONTENT_W, 36, text)
    _style(reqs, oid, 0, len(text), bold=True, size=20, color=NAVY, font=FONT_SERIF)
    _rect(reqs, f"{sid}_ul", sid, MARGIN, TITLE_Y + 38, 56, 2.5, BLUE)


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

    return idx + 1


def _health_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Account Health Snapshot")

    eng = report["engagement"]
    bench = report["benchmarks"]
    acct = report["account"]
    rate = eng["active_rate_7d"]
    active = eng["active_7d"] + eng["active_30d"]
    vs = rate - bench["peer_median_rate"]
    direction = "above" if vs > 0 else "below" if vs < 0 else "at"
    internal = acct.get("internal_visitors", 0)

    # Health badge
    if rate >= 40:
        label, badge_bg = "HEALTHY", {"red": 0.10, "green": 0.55, "blue": 0.28}
    elif rate >= 20:
        label, badge_bg = "MODERATE", BLUE
    else:
        label, badge_bg = "AT RISK", {"red": 0.78, "green": 0.18, "blue": 0.18}
    _pill(reqs, f"{sid}_badge", sid, SLIDE_W - MARGIN - 110, TITLE_Y + 2, 110, 28, label, badge_bg, WHITE)

    # KPIs
    lines = [
        f"Customer Users: {acct['total_visitors']}",
        f"Active This Week: {eng['active_7d']}  ({rate}%)",
        f"Active This Month: {active}",
        f"Dormant (30+ days): {eng['dormant']}",
        "",
        f"Weekly Active Rate: {rate}%  ({abs(vs):.0f}pp {direction} peer median of {bench['peer_median_rate']}%)",
        f"Sites: {acct['total_sites']}  |  Peers benchmarked: {bench['peer_count']}",
    ]
    if internal:
        lines.append(f"({internal} internal staff excluded)")
    kpi = "\n".join(lines)

    _box(reqs, f"{sid}_kpi", sid, MARGIN, BODY_Y, CONTENT_W, 260, kpi)
    _style(reqs, f"{sid}_kpi", 0, len(kpi), size=12, color=NAVY, font=FONT)

    off = 0
    for line in lines:
        if ":" in line and line.strip() and not line.startswith("("):
            c = line.index(":")
            _style(reqs, f"{sid}_kpi", off, off + c + 1, bold=True)
        off += len(line) + 1

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

    sites = report["sites"]
    if not sites:
        _box(reqs, f"{sid}_e", sid, MARGIN, BODY_Y, CONTENT_W, 30, "No site data available")
        _style(reqs, f"{sid}_e", 0, 22, size=12, color=GRAY, font=FONT, italic=True)
        return idx + 1

    hdr = f"{'Site':<26s} {'Users':>5s} {'Pages':>6s} {'Features':>8s} {'Events':>7s} {'Minutes':>7s} {'Last Active':>11s}"
    rows = [hdr]
    for s in sites:
        nm = s["sitename"][:25] if len(s["sitename"]) > 25 else s["sitename"]
        rows.append(
            f"{nm:<26s} {s['visitors']:>5d} {s['page_views']:>6,d} {s['feature_clicks']:>8,d} "
            f"{s['total_events']:>7,d} {s['total_minutes']:>7,d} {s['last_active']:>11s}"
        )
    if len(sites) > 1:
        tv = sum(s["visitors"] for s in sites)
        tp = sum(s["page_views"] for s in sites)
        tf_ = sum(s["feature_clicks"] for s in sites)
        te = sum(s["total_events"] for s in sites)
        tm = sum(s["total_minutes"] for s in sites)
        rows.append("")
        rows.append(f"{'Total':<26s} {tv:>5d} {tp:>6,d} {tf_:>8,d} {te:>7,d} {tm:>7,d}")

    tbl = "\n".join(rows)
    _box(reqs, f"{sid}_tbl", sid, MARGIN, BODY_Y, CONTENT_W, 290, tbl)
    _style(reqs, f"{sid}_tbl", 0, len(tbl), size=9, color=NAVY, font=MONO)
    _style(reqs, f"{sid}_tbl", 0, len(hdr), bold=True, color=BLUE)

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

    champions = report["champions"]
    at_risk = report["at_risk_users"]

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
    for u in at_risk[:6]:
        email = u["email"] or "unknown"
        if len(email) > 32:
            email = email[:29] + "..."
        d = f"{int(u['days_inactive'])}d ago" if u["days_inactive"] < 999 else "never"
        rl.append(f"  {email}")
        rl.append(f"    {u['role']}  ·  {d}")
    if not at_risk:
        rl.append("  All users active!")
    rt = "\n".join(rl)

    _box(reqs, f"{sid}_ri", sid, 380, BODY_Y, 300, 290, rt)
    _style(reqs, f"{sid}_ri", 0, len(rt), size=10, color=NAVY, font=FONT)
    hdr = "At Risk  (30+ days inactive)"
    _style(reqs, f"{sid}_ri", 0, len(hdr), bold=True, size=11, color=BLUE)

    return idx + 1


def _benchmarks_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _slide_title(reqs, sid, "Peer Benchmarks")

    bench = report["benchmarks"]
    acct = report["account"]
    cust_rate = bench["customer_active_rate"]
    med_rate = bench["peer_median_rate"]
    delta = cust_rate - med_rate

    # Big number callout
    big = f"{cust_rate}%"
    _box(reqs, f"{sid}_big", sid, MARGIN, BODY_Y + 8, 160, 50, big)
    _style(reqs, f"{sid}_big", 0, len(big), bold=True, size=36, color=BLUE, font=FONT)

    sub = "weekly active rate"
    _box(reqs, f"{sid}_sub", sid, MARGIN, BODY_Y + 58, 160, 20, sub)
    _style(reqs, f"{sid}_sub", 0, len(sub), size=10, color=GRAY, font=FONT)

    med_big = f"{med_rate}%"
    _box(reqs, f"{sid}_med", sid, 220, BODY_Y + 8, 160, 50, med_big)
    _style(reqs, f"{sid}_med", 0, len(med_big), bold=True, size=36, color=NAVY, font=FONT)

    medsub = "peer median"
    _box(reqs, f"{sid}_ms", sid, 220, BODY_Y + 58, 160, 20, medsub)
    _style(reqs, f"{sid}_ms", 0, len(medsub), size=10, color=GRAY, font=FONT)

    # Context
    lines = [
        f"Delta: {'+' if delta >= 0 else ''}{delta:.0f} percentage points  (across {bench['peer_count']} customers with 5+ users)",
        f"Account size: {acct['total_visitors']} users across {acct['total_sites']} sites",
        "",
    ]
    if delta > 15:
        lines.append("Engagement significantly exceeds peer average.")
        lines.append("Strong candidate for case study, reference, or expansion.")
    elif delta > 0:
        lines.append("Performing above peer average.")
        lines.append("Continue strategy; watch for expansion signals.")
    elif delta > -10:
        lines.append("Near the peer average.")
        lines.append("Monitor for downward trend; proactive outreach recommended.")
    else:
        lines.append("Significantly below peer average.")
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

    fl = ["By Feature"]
    for i, f in enumerate(by_feature[:8], 1):
        name = f["feature"][:36] if len(f["feature"]) > 36 else f["feature"]
        fl.append(f"  {i}. {name}  ({f['exports']:,})")
    if not by_feature:
        fl.append("  No export data")
    ft = "\n".join(fl)
    _box(reqs, f"{sid}_bf", sid, MARGIN, BODY_Y + 24, 340, 270, ft)
    _style(reqs, f"{sid}_bf", 0, len(ft), size=10, color=NAVY, font=FONT)
    _style(reqs, f"{sid}_bf", 0, len("By Feature"), bold=True, size=11, color=BLUE)

    # Right: top exporters
    el = ["Top Exporters"]
    for u in top_exporters:
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


def _signals_slide(reqs, sid, report, idx):
    signals = report.get("signals", [])
    if not signals:
        return idx

    _slide(reqs, sid, idx)
    _bg(reqs, sid, LIGHT)
    _slide_title(reqs, sid, "Notable Signals")

    lines = []
    for i, s in enumerate(signals, 1):
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
    "custom": _custom_slide,
    "signals": _signals_slide,
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
    "custom": ["title", "sections"],
    "signals": ["signals"],
}


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
        if GOOGLE_DRIVE_FOLDER_ID:
            file_meta["parents"] = [GOOGLE_DRIVE_FOLDER_ID]
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

    # Determine insertion index
    try:
        pres = slides_service.presentations().get(presentationId=deck_id).execute()
        idx = len(pres.get("slides", []))
    except Exception:
        idx = 0

    # Unique slide ID
    count = _slide_counter.get(deck_id, 0)
    _slide_counter[deck_id] = count + 1
    sid = f"s_{slide_type}_{count}"

    reqs: list[dict] = []
    new_idx = builder(reqs, sid, data, idx)

    if not reqs:
        return {"slide_type": slide_type, "status": "skipped (no data)"}

    try:
        slides_service.presentations().batchUpdate(
            presentationId=deck_id, body={"requests": reqs},
        ).execute()
    except HttpError as e:
        return {"error": str(e), "slide_type": slide_type}

    return {"slide_type": slide_type, "status": "added", "position": idx + 1}


# ── Monolith deck creation (manifest-driven) ──

def create_health_deck(
    report: dict[str, Any],
    manifest_id: str = "cs_health_review",
) -> dict[str, Any]:
    """Create a deck from a customer health report using a manifest.

    Args:
        report: Full customer health report from PendoClient.get_customer_health_report().
        manifest_id: Which deck manifest to use. Defaults to 'cs_health_review'.
    """
    if "error" in report:
        return {"error": report["error"]}

    customer = report.get("customer", "Unknown")
    days = report.get("days", 30)

    try:
        slides_service, drive_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    from .manifest_loader import resolve_manifest

    resolved = resolve_manifest(manifest_id, customer)
    deck_name = resolved.get("name", "Health Review")
    title = f"{customer} — {deck_name} ({_date_range(days)})"

    try:
        file_meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
        if GOOGLE_DRIVE_FOLDER_ID:
            file_meta["parents"] = [GOOGLE_DRIVE_FOLDER_ID]
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

    return {
        "presentation_id": pres_id,
        "url": f"https://docs.google.com/presentation/d/{pres_id}/edit",
        "customer": customer,
        "slides_created": slides_created,
    }


def create_health_decks_for_customers(
    customer_names: list[str],
    days: int = 30,
    max_customers: int | None = None,
    manifest_id: str = "cs_health_review",
    workers: int = 4,
) -> list[dict[str, Any]]:
    """Create one deck per customer using a manifest (parallel).

    Args:
        customer_names: List of customer names to generate decks for.
        days: Lookback window in days.
        max_customers: Cap on how many to generate.
        manifest_id: Which deck manifest to use (default: cs_health_review).
        workers: Concurrent deck-creation threads (default 4).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .pendo_client import PendoClient

    client = PendoClient()
    client.preload(days)
    customers = customer_names[:max_customers] if max_customers else customer_names

    def _build_one(idx_name: tuple[int, str]) -> dict[str, Any]:
        i, name = idx_name
        logger.info("Generating deck %d/%d: %s (%s)", i + 1, len(customers), name, manifest_id)
        try:
            report = client.get_customer_health_report(name, days=days)
            return create_health_deck(report, manifest_id=manifest_id)
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
        if GOOGLE_DRIVE_FOLDER_ID:
            meta["parents"] = [GOOGLE_DRIVE_FOLDER_ID]
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
