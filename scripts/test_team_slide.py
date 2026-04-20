#!/usr/bin/env python3
"""Generate a single team slide for Bombardier to test the layout."""

import sys
sys.path.insert(0, ".")

from src.slides_client import (
    _get_service, _team_slide, _date_range,
    export_slide_thumbnails,
)
from googleapiclient.errors import HttpError


def main():
    slides_service, drive_service = _get_service()

    # Create a minimal presentation
    title = f"Bombardier — Team Slide Test ({_date_range(30)})"
    from src.config import GOOGLE_QBR_GENERATOR_FOLDER_ID
    meta = {"name": title, "mimeType": "application/vnd.google-apps.presentation"}
    if GOOGLE_QBR_GENERATOR_FOLDER_ID:
        from src.slides_client import _get_deck_output_folder
        folder = _get_deck_output_folder()
        if folder:
            meta["parents"] = [folder]

    f = drive_service.files().create(body=meta).execute()
    pres_id = f["id"]
    print(f"Created: https://docs.google.com/presentation/d/{pres_id}/edit")

    # Build the team slide
    report = {"customer": "Bombardier", "days": 30}
    reqs = []
    _team_slide(reqs, "s_team_1", report, 1)

    # Delete the default blank slide
    pres = slides_service.presentations().get(presentationId=pres_id).execute()
    default_id = pres["slides"][0]["objectId"]
    reqs.append({"deleteObject": {"objectId": default_id}})

    slides_service.presentations().batchUpdate(
        presentationId=pres_id, body={"requests": reqs},
    ).execute()
    print("Team slide created successfully.")

    # Export thumbnail
    try:
        thumbs = export_slide_thumbnails(pres_id)
        for t in thumbs:
            print(f"Thumbnail: {t}")
    except Exception as e:
        print(f"Thumbnail export failed: {e}")


if __name__ == "__main__":
    main()
