-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: Gmail Emails
-- MAGIC Raw ingestion from landing volume

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW bronze_gmail_emails
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM read_files(
  '/Volumes/satsen_catalog/satsen_sa_accounts_claude/landing/gmail_emails/',
  format => 'json',
  inferColumnTypes => true
);
