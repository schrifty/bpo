#!/usr/bin/env python3
"""Generate a single Data Quality slide to test the redesign."""

import sys
sys.path.insert(0, ".")

from src.slides_client import (
    _get_service, _data_quality_slide, _date_range,
    export_slide_thumbnails,
)
from src.qa import qa


def main():
    # Simulate realistic flags
    qa.begin("Bombardier")
    qa.check("engagement buckets sum correctly")
    qa.check("active rate matches")
    qa.check("site count consistent")
    qa.flag("JIRA data unavailable: connection refused",
            sources=("JIRA API",), severity="warning")
    qa.flag("Site count differs: Pendo sees 8 sites, CS Report has 6 factories",
            expected=8, actual=6,
            sources=("Pendo visitor data", "CS Report factory list"),
            severity="info")
    qa.flag("Engagement rate gap: Pendo app login rate 42% vs CS Report buyer engagement 28%",
            expected="within 15pp", actual="14pp difference",
            sources=("Pendo active_rate_7d", "CS Report weeklyActiveBuyersPercent"),
            severity="info")
    qa.check("factory names consistent")
    # This internal flag should NOT appear on the slide
    qa.flag("Drive slides unavailable, using local defaults",
            sources=("Google Drive", "local slides/"),
            severity="warning", internal=True)

    slides_service, drive_service = _get_service()

    title = f"Bombardier — Data Quality Test ({_date_range(30)})"
    from src.slides_client import _get_deck_output_folder
    meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
    folder = _get_deck_output_folder()
    if folder:
        meta["parents"] = [folder]

    f = drive_service.files().create(body=meta).execute()
    pres_id = f["id"]
    print(f"Created: https://docs.google.com/presentation/d/{pres_id}/edit")

    report = {"customer": "Bombardier", "days": 30}
    reqs = []
    _data_quality_slide(reqs, "s_dq_1", report, 1)

    pres = slides_service.presentations().get(presentationId=pres_id).execute()
    default_id = pres["slides"][0]["objectId"]
    reqs.append({"deleteObject": {"objectId": default_id}})

    slides_service.presentations().batchUpdate(
        presentationId=pres_id, body={"requests": reqs},
    ).execute()
    print("Data Quality slide created.")

    try:
        thumbs = export_slide_thumbnails(pres_id)
        for t in thumbs:
            print(f"Thumbnail: {t}")
    except Exception as e:
        print(f"Thumbnail export failed: {e}")


if __name__ == "__main__":
    main()
