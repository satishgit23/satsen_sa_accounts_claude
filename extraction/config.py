# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration for SA Accounts Extraction

# Constants
CATALOG = "satsen_catalog"
SCHEMA = "satsen_sa_accounts_claude"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
SA_NAME = "Satish Senapathy"

# Salesforce
SF_TARGET_ORG = "satish.senapathy@databricks.com"

# Landing subfolders
LANDING_PATHS = {
    "salesforce_accounts": f"{VOLUME_PATH}/salesforce_accounts",
    "salesforce_usecases": f"{VOLUME_PATH}/salesforce_usecases",
    "gmail_emails": f"{VOLUME_PATH}/gmail_emails",
    "google_calendar": f"{VOLUME_PATH}/google_calendar",
    "slack_messages": f"{VOLUME_PATH}/slack_messages",
    "google_drive_docs": f"{VOLUME_PATH}/google_drive_docs",
}
