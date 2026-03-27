-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Slack Messages
-- MAGIC Cleaned messages linked to accounts

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_slack_messages
AS
SELECT
  channel_name,
  channel_id,
  message_text,
  user_id,
  user_name,
  CAST(FROM_UNIXTIME(CAST(timestamp AS DOUBLE)) AS TIMESTAMP) AS message_timestamp,
  thread_ts,
  reply_count,
  -- Extract account name from channel name (channels often named after accounts)
  REPLACE(REPLACE(channel_name, 'ext-', ''), '-', ' ') AS derived_account_name,
  _ingested_at
FROM bronze_slack_messages
WHERE message_text IS NOT NULL
  AND message_text != ''
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY channel_id, timestamp
  ORDER BY _ingested_at DESC
) = 1;
