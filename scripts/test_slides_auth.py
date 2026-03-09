#!/usr/bin/env python3
"""Quick test of Google Slides API auth. Run: python scripts/test_slides_auth.py"""
import json
import os
import sys

# Add project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(project_root) / ".env")

def main():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        print("ERROR: GOOGLE_APPLICATION_CREDENTIALS not set in .env")
        return 1
    if not os.path.exists(creds_path):
        print(f"ERROR: Credentials file not found: {creds_path}")
        return 1
    print(f"Using credentials: {creds_path}")

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError

        SCOPES = [
            "https://www.googleapis.com/auth/presentations",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        # Create via Drive API. If storage quota exceeded, create a folder in YOUR Drive,
        # share it with the service account (Editor), set GOOGLE_DRIVE_FOLDER_ID in .env
        drive = build("drive", "v3", credentials=creds)
        file_meta = {"name": "BPO Test - Delete Me", "mimeType": "application/vnd.google-apps.presentation"}
        folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        owner_email = os.environ.get("GOOGLE_DRIVE_OWNER_EMAIL", "").strip()
        if folder_id:
            file_meta["parents"] = [folder_id]
            print(f"Using folder: {folder_id}")
        else:
            print("No GOOGLE_DRIVE_FOLDER_ID in .env - creating in service account Drive")
        if owner_email:
            creds = creds.with_subject(owner_email)
            drive = build("drive", "v3", credentials=creds)
            print(f"Impersonating: {owner_email} (domain-wide delegation)")
        file = drive.files().create(body=file_meta).execute()
        pres_id = file["id"]
        print(f"SUCCESS! Created via Drive API: https://docs.google.com/presentation/d/{pres_id}/edit")
        return 0
    except HttpError as e:
        print(f"HTTP Error {e.resp.status}: {e}")
        if hasattr(e, "content") and e.content:
            try:
                err_json = json.loads(e.content.decode())
                print("\nFull error response:", json.dumps(err_json, indent=2))
            except Exception:
                pass
        if e.resp.status == 403:
            try:
                with open(creds_path) as f:
                    proj = json.load(f).get("project_id", "bpo-slides")
            except Exception:
                proj = "bpo-slides"
            # Get service account email for gcloud command
            sa_email = ""
            try:
                with open(creds_path) as f:
                    sa_email = json.load(f).get("client_email", "")
            except Exception:
                pass
            if "storageQuotaExceeded" in str(e) or "storage quota" in str(e).lower():
                print("\nFIX: Service account Drive is full. Use domain-wide delegation:")
                print("  1. Create a folder in YOUR Google Drive")
                print("  2. Share it with bpo-slides-account@bpo-slides.iam.gserviceaccount.com (Editor)")
                print("  3. Add to .env: GOOGLE_DRIVE_FOLDER_ID=<folder-id>")
                print("  4. Enable domain-wide delegation (see README) and add GOOGLE_DRIVE_OWNER_EMAIL=<your-email>")
                return 1
            print("\n403 FIX - Run (requires gcloud auth login first):")
            if sa_email:
                print("  # Option A: Service Usage Consumer (minimal)")
                print(f"  gcloud projects add-iam-policy-binding {proj} \\")
                print(f"    --member=serviceAccount:{sa_email} \\")
                print("    --role=roles/serviceusage.serviceUsageConsumer")
                print("\n  # Option B: Editor (broader, if A doesn't work)")
                print(f"  gcloud projects add-iam-policy-binding {proj} \\")
                print(f"    --member=serviceAccount:{sa_email} \\")
                print("    --role=roles/editor")
            print("\nOr manually: IAM -> find service account -> Add role")
            print(f"  https://console.cloud.google.com/iam-admin/iam?project={proj}")
            print("\nAlso ensure: Slides API + Drive API enabled, Billing linked")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
