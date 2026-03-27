-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: Google Drive Documents
-- MAGIC Raw ingestion from landing volume

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW bronze_google_drive_docs
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM read_files(
  '/Volumes/satsen_catalog/satsen_sa_accounts_claude/landing/google_drive_docs/',
  format => 'json',
  inferColumnTypes => true
);
