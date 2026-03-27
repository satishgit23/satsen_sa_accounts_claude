-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: Salesforce Accounts
-- MAGIC Raw ingestion from landing volume

CREATE OR REFRESH MATERIALIZED VIEW bronze_salesforce_accounts
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM read_files(
  '/Volumes/satsen_catalog/satsen_sa_accounts_claude/landing/salesforce_accounts/',
  format => 'json',
  inferColumnTypes => true
);
