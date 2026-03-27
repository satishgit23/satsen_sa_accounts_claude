# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Utilities for SA Accounts Extraction

import json
import os
from datetime import datetime


def write_json_to_volume(data, volume_path, filename=None):
    """Write JSON data to a UC Volume landing path."""
    if filename is None:
        filename = f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    os.makedirs(volume_path, exist_ok=True)
    filepath = os.path.join(volume_path, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"Wrote {len(data)} records to {filepath}")
    return filepath


def get_secret(scope, key):
    """Get a secret from Databricks secret scope."""
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        return os.environ.get(key, "")


def timestamp_now():
    """Return current UTC timestamp as ISO string."""
    return datetime.utcnow().isoformat() + "Z"
