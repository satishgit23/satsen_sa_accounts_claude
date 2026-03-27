# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Salesforce Accounts
# MAGIC Queries accounts where Last SA Engaged = Satish Senapathy

import json
import subprocess
from datetime import datetime

# Config
SA_USER_ID = "005Vp00000BXL2XIAX"  # Satish Senapathy
TARGET_ORG = "satish.senapathy@databricks.com"
VOLUME_PATH = "/Volumes/satsen_catalog/satsen_sa_accounts_claude/landing/salesforce_accounts"

# COMMAND ----------

def run_sf_query(soql):
    """Execute a Salesforce SOQL query via sf CLI."""
    result = subprocess.run(
        ["sf", "data", "query", "--query", soql,
         "--target-org", TARGET_ORG, "--json"],
        capture_output=True, text=True
    )
    data = json.loads(result.stdout)
    return data.get("result", {}).get("records", [])


def clean_records(records):
    """Flatten and clean Salesforce records."""
    cleaned = []
    for r in records:
        r.pop("attributes", None)
        if "Owner" in r and isinstance(r["Owner"], dict):
            r["Owner_Name"] = r["Owner"].get("Name", "")
            del r["Owner"]
        if "Last_SA_Engaged__r" in r:
            del r["Last_SA_Engaged__r"]
        cleaned.append(r)
    return cleaned

# COMMAND ----------

# Query accounts
query = f"""
SELECT Id, Name, Industry, Type, BillingCity, BillingState, BillingCountry,
       Owner.Name, CreatedDate, LastModifiedDate
FROM Account
WHERE Last_SA_Engaged__c = '{SA_USER_ID}'
"""

records = run_sf_query(query)
records = clean_records(records)
print(f"Extracted {len(records)} accounts")

# COMMAND ----------

# Write to landing volume
import os
os.makedirs(VOLUME_PATH, exist_ok=True)
filename = f"accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
filepath = os.path.join(VOLUME_PATH, filename)

with open(filepath, "w") as f:
    json.dump(records, f, indent=2, default=str)

print(f"Wrote {len(records)} accounts to {filepath}")
