"""Google Slides client for creating CS-oriented usage report decks."""

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

# ── Two-color palette ──
NAVY = {"red": 0.12, "green": 0.22, "blue": 0.37}
AMBER = {"red": 0.83, "green": 0.52, "blue": 0.11}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
GRAY = {"red": 0.55, "green": 0.57, "blue": 0.60}
LIGHT = {"red": 0.96, "green": 0.96, "blue": 0.97}
FONT = "Google Sans"
MONO = "Roboto Mono"


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
    """Standard content-slide title: navy text + amber underline."""
    oid = f"{sid}_ttl"
    _box(reqs, oid, sid, MARGIN, TITLE_Y, CONTENT_W, 36, text)
    _style(reqs, oid, 0, len(text), bold=True, size=20, color=NAVY, font=FONT)
    _rect(reqs, f"{sid}_ul", sid, MARGIN, TITLE_Y + 38, 56, 2.5, AMBER)


# ── Slide builders ──

def _title_slide(reqs, sid, report, idx):
    _slide(reqs, sid, idx)
    _bg(reqs, sid, NAVY)

    acct = report["account"]
    name = report["customer"]
    sub = f"Product Usage Review  ·  Last {report['days']} Days"
    meta = f"CSM: {acct['csm']}  |  {acct['total_sites']} sites · {acct['total_visitors']} users  |  {report['generated']}"

    _rect(reqs, f"{sid}_bar", sid, 0, 190, SLIDE_W, 3, AMBER)

    _box(reqs, f"{sid}_n", sid, MARGIN, 100, CONTENT_W, 60, name)
    _style(reqs, f"{sid}_n", 0, len(name), bold=True, size=40, color=WHITE, font=FONT)

    _box(reqs, f"{sid}_s", sid, MARGIN, 200, CONTENT_W, 30, sub)
    _style(reqs, f"{sid}_s", 0, len(sub), size=15, color=AMBER, font=FONT)

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
        label, badge_bg = "MODERATE", AMBER
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
    _style(reqs, f"{sid}_roles", 0, len(ah), bold=True, size=11, color=AMBER)
    dh = "Dormant Roles"
    di = role_text.index(dh)
    _style(reqs, f"{sid}_roles", di, di + len(dh), bold=True, size=11, color=AMBER)

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
    _style(reqs, f"{sid}_tbl", 0, len(hdr), bold=True, color=AMBER)

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
    _style(reqs, f"{sid}_pg", 0, len("Top Pages"), bold=True, size=11, color=AMBER)

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
    _style(reqs, f"{sid}_ft", 0, len("Top Features"), bold=True, size=11, color=AMBER)

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
    _style(reqs, f"{sid}_ch", 0, len("Champions"), bold=True, size=11, color=AMBER)

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
    _style(reqs, f"{sid}_ri", 0, len(hdr), bold=True, size=11, color=AMBER)

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
    _style(reqs, f"{sid}_big", 0, len(big), bold=True, size=36, color=AMBER, font=FONT)

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
            _style(reqs, f"{sid}_sig", off, off + dot + 1, bold=True, color=AMBER)
        off += len(line) + 1

    return idx + 1


# ── Main deck creation ──

def create_health_deck(report: dict[str, Any]) -> dict[str, Any]:
    """Create a CS-oriented health deck from a customer health report."""
    if "error" in report:
        return {"error": report["error"]}

    customer = report.get("customer", "Unknown")
    days = report.get("days", 30)

    try:
        slides_service, drive_service = _get_service()
    except (ValueError, FileNotFoundError) as e:
        return {"error": str(e)}

    title = f"{customer} — Usage Health Review (Last {days} Days)"
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

    reqs: list[dict] = []
    idx = 1

    idx = _title_slide(reqs, "s_title", report, idx)
    idx = _health_slide(reqs, "s_health", report, idx)
    idx = _engagement_slide(reqs, "s_engage", report, idx)
    idx = _sites_slide(reqs, "s_sites", report, idx)
    idx = _features_slide(reqs, "s_feat", report, idx)
    idx = _champions_slide(reqs, "s_champ", report, idx)
    idx = _benchmarks_slide(reqs, "s_bench", report, idx)
    idx = _signals_slide(reqs, "s_sig", report, idx)

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
    delay_seconds: float = 3.0,
    max_customers: int | None = None,
) -> list[dict[str, Any]]:
    """Create one health deck per customer."""
    from .pendo_client import PendoClient

    client = PendoClient()
    customers = customer_names[:max_customers] if max_customers else customer_names
    results = []

    for i, name in enumerate(customers):
        if i > 0:
            time.sleep(delay_seconds)
        logger.info("Generating health deck %d/%d: %s", i + 1, len(customers), name)
        try:
            report = client.get_customer_health_report(name, days=days)
            result = create_health_deck(report)
        except Exception as e:
            result = {"error": str(e), "customer": name}
        results.append(result)
        if "error" in result and "403" in str(result.get("error", "")):
            results.append({"error": "Stopped: 403. Fix auth then retry.", "customers_attempted": i + 1})
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
    title = f"{customer} - Usage Report (Last {days} days)"
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
