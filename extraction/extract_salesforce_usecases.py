# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Salesforce Use Cases
# MAGIC Queries use cases for all SA-engaged accounts

import json
import subprocess
from datetime import datetime

# Config
SA_USER_ID = "005Vp00000BXL2XIAX"  # Satish Senapathy
TARGET_ORG = "satish.senapathy@databricks.com"
VOLUME_PATH = "/Volumes/satsen_catalog/satsen_sa_accounts_claude/landing/salesforce_usecases"
ACCOUNTS_PATH = "/Volumes/satsen_catalog/satsen_sa_accounts_claude/landing/salesforce_accounts"

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

# COMMAND ----------

# Get account IDs from accounts landing
import glob
account_files = sorted(glob.glob(f"{ACCOUNTS_PATH}/*.json"))
account_ids = []
for af in account_files:
    with open(af) as f:
        accts = json.load(f)
    account_ids.extend([a["Id"] for a in accts])

account_ids = list(set(account_ids))
print(f"Found {len(account_ids)} unique account IDs")

# COMMAND ----------

# Query use cases
id_str = ",".join([f"'{i}'" for i in account_ids])
query = f"""
SELECT Id, Name, Account__c, Account__r.Name,
       Stages__c, Concatenated_Stage_Name__c, Status__c,
       Description__c, Use_Case_Description__c,
       Business_Use_Case__c, Use_Case_Area__c, Workload_Type__c,
       Use_Case_Type__c, Go_Live_Date__c, Stage_Numeric__c,
       CreatedDate, LastModifiedDate
FROM UseCase__c
WHERE Account__c IN ({id_str})
"""

records = run_sf_query(query)

# Flatten
for rec in records:
    rec.pop("attributes", None)
    if "Account__r" in rec and isinstance(rec["Account__r"], dict):
        rec["Account_Name"] = rec["Account__r"].get("Name", "")
        rec["Account_Id"] = rec.get("Account__c", "")
        del rec["Account__r"]
    rec["Stage"] = rec.pop("Stages__c", None) or rec.pop("Concatenated_Stage_Name__c", None)
    rec["Status"] = rec.pop("Status__c", None)
    rec["Description"] = rec.pop("Use_Case_Description__c", None) or rec.pop("Description__c", None)
    rec["Product_Area"] = rec.pop("Use_Case_Area__c", None)
    rec["Workload"] = rec.pop("Workload_Type__c", None)

print(f"Extracted {len(records)} use cases")

# COMMAND ----------

# Write to landing volume
import os
os.makedirs(VOLUME_PATH, exist_ok=True)
filename = f"usecases_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
filepath = os.path.join(VOLUME_PATH, filename)

with open(filepath, "w") as f:
    json.dump(records, f, indent=2, default=str)

print(f"Wrote {len(records)} use cases to {filepath}")
