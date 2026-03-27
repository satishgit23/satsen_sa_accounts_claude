#!/usr/bin/env python3
"""
Extract Gmail emails with the "Customers" label for SA accounts tracking.
Uses Google auth module and Gmail API.
"""

import json
import subprocess
import sys
import time
import urllib.parse

GOOGLE_AUTH_SCRIPT = "/Users/satish.senapathy/.vibe/marketplace/plugins/fe-google-tools/skills/google-auth/resources/google_auth.py"
QUOTA_PROJECT = "gcp-sandbox-field-eng"
OUTPUT_FILE = "/Users/satish.senapathy/claude_isaac/satsen_sa_accounts_claude/extraction/gmail_emails_raw.json"
MAX_RESULTS = 100


def get_token():
    """Get OAuth token using the Google auth module."""
    result = subprocess.run(
        [sys.executable, GOOGLE_AUTH_SCRIPT, "token"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"ERROR: Failed to get token: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def gmail_api(token, endpoint):
    """Make a Gmail API call and return parsed JSON."""
    url = f"https://gmail.googleapis.com/gmail/v1/users/me/{endpoint}"
    result = subprocess.run(
        ["curl", "-s", url,
         "-H", f"Authorization: Bearer {token}",
         "-H", f"x-goog-user-project: {QUOTA_PROJECT}"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"ERROR: curl failed for {endpoint}: {result.stderr}", file=sys.stderr)
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON from {endpoint}: {result.stdout[:200]}", file=sys.stderr)
        return None


def find_customers_labels(token):
    """Find all label IDs under the 'Customers' hierarchy."""
    data = gmail_api(token, "labels")
    if not data or "labels" not in data:
        print("ERROR: Could not retrieve labels", file=sys.stderr)
        sys.exit(1)

    # Get all labels whose name starts with "Customers" (parent + sub-labels)
    matches = [l for l in data["labels"]
               if l["name"].lower() == "customers" or l["name"].lower().startswith("customers/")]
    if not matches:
        print("ERROR: No label matching 'Customers' found.", file=sys.stderr)
        print("Available labels:", file=sys.stderr)
        for l in sorted(data["labels"], key=lambda x: x["name"]):
            print(f"  {l['name']} ({l['id']})", file=sys.stderr)
        sys.exit(1)

    return matches


def get_message_ids(token, label_id):
    """Get message IDs for the given label (last 90 days, up to MAX_RESULTS)."""
    # Use q parameter to filter to last 90 days
    query = urllib.parse.quote("newer_than:90d")
    endpoint = f"messages?labelIds={label_id}&maxResults={MAX_RESULTS}&q={query}"
    data = gmail_api(token, endpoint)
    if not data:
        return []

    if "error" in data:
        print(f"ERROR: API error: {json.dumps(data['error'], indent=2)}", file=sys.stderr)
        return []

    messages = data.get("messages", [])

    # Handle pagination if needed
    while "nextPageToken" in data and len(messages) < MAX_RESULTS:
        next_token = data["nextPageToken"]
        data = gmail_api(token, f"{endpoint}&pageToken={next_token}")
        if not data or "messages" not in data:
            break
        messages.extend(data["messages"])

    return messages[:MAX_RESULTS]


def get_message_details(token, msg_id):
    """Get metadata for a single message."""
    endpoint = (
        f"messages/{msg_id}?format=metadata"
        "&metadataHeaders=Subject"
        "&metadataHeaders=From"
        "&metadataHeaders=To"
        "&metadataHeaders=Date"
    )
    return gmail_api(token, endpoint)


def extract_header(headers, name):
    """Extract a header value by name."""
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def main():
    print("Step 1: Authenticating...")
    token = get_token()
    print(f"  Token obtained (length={len(token)})")

    print("Step 2: Finding 'Customers' labels...")
    labels = find_customers_labels(token)
    print(f"  Found {len(labels)} Customers labels:")
    for lbl in labels:
        print(f"    - {lbl['name']} ({lbl['id']})")

    # Build a label_id -> label_name map for enrichment
    label_id_to_name = {lbl["id"]: lbl["name"] for lbl in labels}

    # Query all Customers labels (parent + sub-labels) and deduplicate by message ID
    print(f"\nStep 3: Querying messages across all Customers labels (last 90 days, max {MAX_RESULTS})...")
    seen_ids = set()
    messages = []
    for lbl in labels:
        label_msgs = get_message_ids(token, lbl["id"])
        print(f"  {lbl['name']}: {len(label_msgs)} messages")
        for m in label_msgs:
            if m["id"] not in seen_ids:
                seen_ids.add(m["id"])
                messages.append(m)
    # Cap at MAX_RESULTS
    messages = messages[:MAX_RESULTS]
    print(f"  Total unique messages: {len(messages)}")

    if not messages:
        print("No messages found. Saving empty result.")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f, indent=2)
        return

    print(f"Step 4: Fetching details for {len(messages)} messages...")
    results = []
    for i, msg in enumerate(messages):
        msg_id = msg["id"]
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  Processing message {i+1}/{len(messages)}...")

        details = get_message_details(token, msg_id)
        if not details:
            print(f"  WARNING: Could not fetch message {msg_id}", file=sys.stderr)
            continue

        headers = details.get("payload", {}).get("headers", [])
        label_ids = details.get("labelIds", [])

        # Resolve customer label names from IDs
        customer_labels = [label_id_to_name[lid] for lid in label_ids if lid in label_id_to_name]

        record = {
            "message_id": msg_id,
            "subject": extract_header(headers, "Subject"),
            "from_email": extract_header(headers, "From"),
            "to_email": extract_header(headers, "To"),
            "date": extract_header(headers, "Date"),
            "snippet": details.get("snippet", ""),
            "labels": label_ids,
            "customer_labels": customer_labels,
            "is_unread": "UNREAD" in label_ids,
        }
        results.append(record)

        # Small delay to avoid rate limiting
        if (i + 1) % 20 == 0:
            time.sleep(0.5)

    print(f"\nStep 5: Saving {len(results)} emails to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Summary
    total = len(results)
    unread = sum(1 for r in results if r["is_unread"])
    print(f"\n{'='*50}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*50}")
    print(f"Total emails extracted: {total}")
    print(f"Unread emails:          {unread}")
    print(f"Read emails:            {total - unread}")
    print(f"Output file:            {OUTPUT_FILE}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
