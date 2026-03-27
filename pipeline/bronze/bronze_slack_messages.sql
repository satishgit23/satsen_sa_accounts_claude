-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: Slack Messages
-- MAGIC Raw ingestion from landing volume

CREATE OR REFRESH MATERIALIZED VIEW bronze_slack_messages
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM read_files(
  '/Volumes/satsen_catalog/satsen_sa_accounts_claude/landing/slack_messages/',
  format => 'json',
  inferColumnTypes => true
);
