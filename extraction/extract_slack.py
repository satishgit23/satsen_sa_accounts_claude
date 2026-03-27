#!/usr/bin/env python3
"""
extract_slack.py - Extract Slack messages for SA accounts tracking.

Extracts messages from Slack channels the user is part of, focusing on
account-related discussions. Requires a SLACK_BOT_TOKEN with the following
OAuth scopes:
  - channels:history
  - channels:read
  - groups:history
  - groups:read
  - users:read
  - search:read (optional, for message search)

Token discovery order:
  1. SLACK_BOT_TOKEN environment variable
  2. SLACK_TOKEN environment variable
  3. ~/.vibe/slack_token file
  4. Falls back to generating sample data

Required Slack API calls:
  - conversations.list   -> list channels the bot/user is in
  - conversations.history -> fetch messages from each channel
  - users.info           -> resolve user IDs to display names
  - conversations.replies -> fetch threaded replies

Output schema per message:
  {
    "channel_name": str,
    "channel_id": str,
    "message_text": str,
    "user_id": str,
    "user_name": str,
    "timestamp": str (ISO 8601),
    "ts": str (Slack ts),
    "thread_ts": str or null,
    "reply_count": int
  }
"""

import json
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent
OUTPUT_FILE = OUTPUT_DIR / "slack_messages_raw.json"

# How far back to look (days)
LOOKBACK_DAYS = 90

# Channel name patterns to prioritize (case-insensitive substring match)
CHANNEL_FILTERS = [
    "account",
    "customer",
    "sa-",
    "solution",
    "deal",
    "opportunity",
    "strategic",
    "enterprise",
    "field",
]


def find_slack_token() -> str | None:
    """Discover a Slack token from environment or config files."""
    # 1. Environment variables
    for var in ("SLACK_BOT_TOKEN", "SLACK_TOKEN"):
        token = os.environ.get(var)
        if token:
            logger.info("Found Slack token in env var %s", var)
            return token

    # 2. Config file locations
    config_paths = [
        Path.home() / ".vibe" / "slack_token",
        Path.home() / ".slack" / "token",
        Path.home() / ".config" / "slack" / "token",
    ]
    for path in config_paths:
        if path.is_file():
            token = path.read_text().strip()
            if token:
                logger.info("Found Slack token in %s", path)
                return token

    return None


def fetch_slack_messages(token: str) -> list[dict]:
    """
    Fetch messages from Slack using the Web API.

    Uses urllib so there are no external dependencies beyond the stdlib.
    """
    import urllib.request
    import urllib.error
    import urllib.parse

    base = "https://slack.com/api"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    def _api_get(method: str, params: dict | None = None) -> dict:
        url = f"{base}/{method}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        if not data.get("ok"):
            logger.warning("Slack API error on %s: %s", method, data.get("error"))
        return data

    # --- Build user cache ---
    user_cache: dict[str, str] = {}

    def resolve_user(uid: str) -> str:
        if uid in user_cache:
            return user_cache[uid]
        try:
            resp = _api_get("users.info", {"user": uid})
            name = resp.get("user", {}).get("real_name") or resp.get("user", {}).get("name", uid)
        except Exception:
            name = uid
        user_cache[uid] = name
        return name

    # --- List channels ---
    logger.info("Fetching channel list...")
    channels_resp = _api_get("conversations.list", {
        "types": "public_channel,private_channel",
        "limit": 200,
        "exclude_archived": "true",
    })
    channels = channels_resp.get("channels", [])
    logger.info("Found %d channels", len(channels))

    # Filter to relevant channels if we have many
    if len(channels) > 20:
        filtered = [
            ch for ch in channels
            if any(f in ch.get("name", "").lower() for f in CHANNEL_FILTERS)
        ]
        if filtered:
            logger.info("Filtered to %d account-related channels", len(filtered))
            channels = filtered

    # --- Fetch messages ---
    oldest = str(int((datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp()))
    messages: list[dict] = []

    for ch in channels:
        ch_id = ch["id"]
        ch_name = ch.get("name", ch_id)
        logger.info("Fetching messages from #%s ...", ch_name)

        cursor = None
        while True:
            params = {
                "channel": ch_id,
                "oldest": oldest,
                "limit": 200,
            }
            if cursor:
                params["cursor"] = cursor

            hist = _api_get("conversations.history", params)
            for msg in hist.get("messages", []):
                user_id = msg.get("user", "")
                ts_float = float(msg.get("ts", 0))
                messages.append({
                    "channel_name": ch_name,
                    "channel_id": ch_id,
                    "message_text": msg.get("text", ""),
                    "user_id": user_id,
                    "user_name": resolve_user(user_id) if user_id else "",
                    "timestamp": datetime.fromtimestamp(ts_float, tz=timezone.utc).isoformat(),
                    "ts": msg.get("ts", ""),
                    "thread_ts": msg.get("thread_ts"),
                    "reply_count": msg.get("reply_count", 0),
                })

            cursor = hist.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

    logger.info("Collected %d messages total", len(messages))
    return messages


def generate_sample_data() -> list[dict]:
    """Generate realistic sample data to demonstrate the output schema."""
    logger.info("No Slack token found. Generating sample data for schema demonstration.")

    now = datetime.now(timezone.utc)
    samples = [
        {
            "channel_name": "sa-accounts-west",
            "channel_id": "C0SAMPLE01",
            "message_text": "Update on Acme Corp: they're moving to production next week with the lakehouse platform. POC completed successfully.",
            "user_id": "U0SAMPLE01",
            "user_name": "Jane Smith",
            "timestamp": (now - timedelta(days=2, hours=3)).isoformat(),
            "ts": str((now - timedelta(days=2, hours=3)).timestamp()),
            "thread_ts": None,
            "reply_count": 3,
        },
        {
            "channel_name": "sa-accounts-west",
            "channel_id": "C0SAMPLE01",
            "message_text": "Great news! What's the expected DBU consumption for Acme?",
            "user_id": "U0SAMPLE02",
            "user_name": "Bob Johnson",
            "timestamp": (now - timedelta(days=2, hours=2)).isoformat(),
            "ts": str((now - timedelta(days=2, hours=2)).timestamp()),
            "thread_ts": str((now - timedelta(days=2, hours=3)).timestamp()),
            "reply_count": 0,
        },
        {
            "channel_name": "strategic-accounts",
            "channel_id": "C0SAMPLE02",
            "message_text": "QBR with GlobalTech scheduled for next Thursday. Need to prep the consumption dashboard.",
            "user_id": "U0SAMPLE03",
            "user_name": "Alice Chen",
            "timestamp": (now - timedelta(days=1, hours=5)).isoformat(),
            "ts": str((now - timedelta(days=1, hours=5)).timestamp()),
            "thread_ts": None,
            "reply_count": 1,
        },
        {
            "channel_name": "customer-escalations",
            "channel_id": "C0SAMPLE03",
            "message_text": "DataFlow Inc is hitting performance issues on their ETL pipeline. Filed ES ticket ES-987654. Working with support.",
            "user_id": "U0SAMPLE01",
            "user_name": "Jane Smith",
            "timestamp": (now - timedelta(hours=8)).isoformat(),
            "ts": str((now - timedelta(hours=8)).timestamp()),
            "thread_ts": None,
            "reply_count": 5,
        },
        {
            "channel_name": "deal-support",
            "channel_id": "C0SAMPLE04",
            "message_text": "Need technical validation for MegaCorp's Unity Catalog migration. They want to consolidate 3 workspaces.",
            "user_id": "U0SAMPLE04",
            "user_name": "Carlos Rivera",
            "timestamp": (now - timedelta(hours=4)).isoformat(),
            "ts": str((now - timedelta(hours=4)).timestamp()),
            "thread_ts": None,
            "reply_count": 2,
        },
    ]
    return samples


def main():
    token = find_slack_token()

    if token:
        messages = fetch_slack_messages(token)
    else:
        logger.warning(
            "No Slack token found. Set SLACK_BOT_TOKEN env var or place token in ~/.vibe/slack_token"
        )
        logger.info("Writing sample data to demonstrate expected output format.")
        messages = generate_sample_data()

    # Sort by timestamp descending
    messages.sort(key=lambda m: m.get("timestamp", ""), reverse=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(messages, f, indent=2, ensure_ascii=False)

    logger.info("Wrote %d messages to %s", len(messages), OUTPUT_FILE)
    return messages


if __name__ == "__main__":
    main()
