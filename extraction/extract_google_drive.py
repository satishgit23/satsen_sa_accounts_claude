#!/usr/bin/env python3
"""Extract Google Drive data from all 'Accounts' folders for SA accounts tracking project."""

import subprocess
import json
import requests
import sys
from datetime import datetime, timezone


def get_google_token():
    """Get Google auth token using the google-auth plugin."""
    r = subprocess.run(
        ["python3", "/Users/satish.senapathy/.vibe/marketplace/plugins/fe-google-tools/skills/google-auth/resources/google_auth.py", "token"],
        capture_output=True, text=True
    )
    token = r.stdout.strip()
    if not token:
        raise RuntimeError(f"Failed to get Google auth token. stderr: {r.stderr}")
    return token


HEADERS = {}
BASE_URL = "https://www.googleapis.com/drive/v3"


def drive_get(endpoint, params=None):
    """Make a GET request to Google Drive API."""
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()


def find_all_accounts_folders():
    """Search for all folders named 'Accounts' in Google Drive and get parent context."""
    all_folders = []
    page_token = None
    while True:
        params = {
            "q": "name='Accounts' and mimeType='application/vnd.google-apps.folder' and trashed=false",
            "fields": "nextPageToken,files(id,name,parents)",
        }
        if page_token:
            params["pageToken"] = page_token
        data = drive_get("files", params)
        all_folders.extend(data.get("files", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    # Resolve parent folder names for context
    for folder in all_folders:
        parents = folder.get("parents", [])
        if parents:
            try:
                parent_info = drive_get(f"files/{parents[0]}", {"fields": "name"})
                folder["parent_name"] = parent_info.get("name", "Unknown")
            except Exception:
                folder["parent_name"] = "Unknown"
        else:
            folder["parent_name"] = "Root"

    print(f"Found {len(all_folders)} folder(s) named 'Accounts':")
    for f in all_folders:
        print(f"  - {f['name']} (parent: {f['parent_name']}, id: {f['id']})")
    return all_folders


def list_files_in_folder(folder_id):
    """List all files in a folder (non-recursive, single page at a time)."""
    all_files = []
    page_token = None
    while True:
        params = {
            "q": f"'{folder_id}' in parents and trashed=false",
            "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,createdTime,size)",
            "pageSize": 100,
        }
        if page_token:
            params["pageToken"] = page_token
        data = drive_get("files", params)
        all_files.extend(data.get("files", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return all_files


def export_google_doc_text(file_id, mime_type):
    """Export a Google Doc/Slides as plain text, or Sheets as CSV."""
    export_mime = "text/plain"
    if mime_type == "application/vnd.google-apps.spreadsheet":
        export_mime = "text/csv"

    url = f"{BASE_URL}/files/{file_id}/export"
    params = {"mimeType": export_mime}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 200:
        return resp.text
    else:
        print(f"    Warning: Could not export doc {file_id}: {resp.status_code}")
        return None


def crawl_folder_recursive(folder_id, folder_name, seen_ids, depth=0):
    """Recursively crawl a folder and collect file info, deduplicating by file_id."""
    indent = "  " * depth
    print(f"{indent}[folder] {folder_name}/")
    files = list_files_in_folder(folder_id)
    results = []
    subfolder_structure = {}

    for f in files:
        mime = f.get("mimeType", "")
        if mime == "application/vnd.google-apps.folder":
            # Recurse into subfolder
            sub_results, sub_structure = crawl_folder_recursive(f["id"], f["name"], seen_ids, depth + 1)
            results.extend(sub_results)
            subfolder_structure[f["name"]] = sub_structure
        else:
            # Skip duplicates
            if f["id"] in seen_ids:
                continue
            seen_ids.add(f["id"])

            print(f"{indent}  [file] {f['name']} ({mime})")
            content = None
            summary = None

            # For Google Docs, Sheets, Slides - export as text
            exportable_types = {
                "application/vnd.google-apps.document",
                "application/vnd.google-apps.spreadsheet",
                "application/vnd.google-apps.presentation",
            }

            if mime in exportable_types:
                text = export_google_doc_text(f["id"], mime)
                if text:
                    content = text[:5000]
                    summary = text[:500]

            record = {
                "file_id": f["id"],
                "file_name": f["name"],
                "folder_name": folder_name,
                "mime_type": mime,
                "modified_date": f.get("modifiedTime"),
                "created_date": f.get("createdTime"),
                "size": f.get("size"),
                "content": content,
                "summary": summary,
            }
            results.append(record)

    file_count = len([f for f in files if f.get("mimeType") != "application/vnd.google-apps.folder"])
    subfolder_structure["_file_count"] = file_count
    return results, subfolder_structure


def main():
    global HEADERS
    print("Getting Google auth token...")
    token = get_google_token()
    HEADERS = {
        "Authorization": f"Bearer {token}",
        "x-goog-user-project": "gcp-sandbox-field-eng",
    }

    print("\nSearching for all 'Accounts' folders...")
    all_accounts_folders = find_all_accounts_folders()

    all_docs = []
    combined_structure = {}
    seen_ids = set()

    for folder in all_accounts_folders:
        label = f"{folder['parent_name']}/{folder['name']}"
        print(f"\n{'='*60}")
        print(f"Crawling: {label} (id: {folder['id']})")
        print(f"{'='*60}\n")
        docs, structure = crawl_folder_recursive(folder["id"], folder["name"], seen_ids, depth=0)
        all_docs.extend(docs)
        combined_structure[label] = structure

    output = {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "accounts_folders_count": len(all_accounts_folders),
        "total_files": len(all_docs),
        "folder_structure": combined_structure,
        "documents": all_docs,
    }

    output_path = "/Users/satish.senapathy/claude_isaac/satsen_sa_accounts_claude/extraction/google_drive_docs_raw.json"
    with open(output_path, "w") as fout:
        json.dump(output, fout, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"Total Accounts folders crawled: {len(all_accounts_folders)}")
    print(f"Total unique files found: {len(all_docs)}")
    print(f"Output saved to: {output_path}")

    # Print summary of folder structure
    print(f"\nFolder structure summary:")
    for label, structure in combined_structure.items():
        subfolders = [k for k in structure.keys() if k != "_file_count"]
        file_count = structure.get("_file_count", 0)
        print(f"\n  {label}: {file_count} direct files, {len(subfolders)} subfolders")
        for sf in sorted(subfolders):
            sf_files = structure[sf].get("_file_count", 0)
            sf_subs = [k for k in structure[sf].keys() if k != "_file_count"]
            print(f"    - {sf}/ ({sf_files} files, {len(sf_subs)} subfolders)")


if __name__ == "__main__":
    main()
