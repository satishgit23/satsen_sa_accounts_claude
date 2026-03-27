#!/usr/bin/env python3
"""Extract Google Calendar events for SA accounts tracking project."""

import subprocess
import json
import requests
from datetime import datetime, timedelta, timezone

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

def fetch_calendar_events(token):
    """Fetch calendar events from the last 90 days through the next 14 days."""
    now = datetime.now(timezone.utc)
    time_min = (now - timedelta(days=90)).isoformat()
    time_max = (now + timedelta(days=14)).isoformat()

    url = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
    headers = {
        "Authorization": f"Bearer {token}",
        "x-goog-user-project": "gcp-sandbox-field-eng",
    }
    params = {
        "timeMin": time_min,
        "timeMax": time_max,
        "maxResults": 500,
        "singleEvents": "true",
        "orderBy": "startTime",
    }

    all_events = []
    page_token = None

    while True:
        if page_token:
            params["pageToken"] = page_token

        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        for event in data.get("items", []):
            start = event.get("start", {})
            end = event.get("end", {})
            attendees_raw = event.get("attendees", [])
            organizer = event.get("organizer", {})

            extracted = {
                "event_id": event.get("id"),
                "summary": event.get("summary"),
                "description": event.get("description"),
                "start_time": start.get("dateTime") or start.get("date"),
                "end_time": end.get("dateTime") or end.get("date"),
                "attendees": [a.get("email") for a in attendees_raw if a.get("email")],
                "organizer": organizer.get("email"),
                "location": event.get("location"),
                "status": event.get("status"),
                "created": event.get("created"),
                "updated": event.get("updated"),
            }
            all_events.append(extracted)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return all_events

def main():
    print("Getting Google auth token...")
    token = get_google_token()

    print("Fetching calendar events...")
    events = fetch_calendar_events(token)

    output_path = "/Users/satish.senapathy/claude_isaac/satsen_sa_accounts_claude/extraction/google_calendar_raw.json"
    with open(output_path, "w") as f:
        json.dump(events, f, indent=2)

    print(f"Saved {len(events)} events to {output_path}")

if __name__ == "__main__":
    main()
