-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Open Items Tracker
-- MAGIC All pending/open items across all sources

CREATE OR REFRESH MATERIALIZED VIEW gold_open_items_tracker
AS

-- Open email items (unread or flagged as action needed)
SELECT
  'Email' AS source,
  message_id AS item_id,
  subject AS item_title,
  snippet AS item_detail,
  from_email AS contact,
  email_date AS item_date,
  CASE WHEN is_unread THEN 'Unread' ELSE 'Action Required' END AS item_status,
  sender_domain AS account_hint
FROM silver_emails
WHERE is_open_item = true

UNION ALL

-- Upcoming meetings (next 14 days) needing prep
SELECT
  'Calendar' AS source,
  event_id AS item_id,
  event_title AS item_title,
  agenda AS item_detail,
  organizer AS contact,
  start_time AS item_date,
  'Upcoming Meeting' AS item_status,
  NULL AS account_hint
FROM silver_calendar_events
WHERE start_time >= current_timestamp()
  AND start_time <= current_timestamp() + INTERVAL 14 DAYS

UNION ALL

-- Active use cases needing attention (U2/U3 stages)
SELECT
  'Salesforce' AS source,
  usecase_id AS item_id,
  CONCAT(account_name, ' - ', usecase_name) AS item_title,
  description AS item_detail,
  NULL AS contact,
  last_modified_date AS item_date,
  CONCAT('Stage: ', usecase_stage) AS item_status,
  account_name AS account_hint
FROM silver_usecases
WHERE usecase_stage IN ('U2', 'U3', 'U4')

UNION ALL

-- Recent unresponded Slack messages (last 7 days)
SELECT
  'Slack' AS source,
  CONCAT(channel_id, '_', CAST(message_timestamp AS STRING)) AS item_id,
  CONCAT('Slack: ', channel_name) AS item_title,
  message_text AS item_detail,
  user_name AS contact,
  message_timestamp AS item_date,
  'Needs Response' AS item_status,
  derived_account_name AS account_hint
FROM silver_slack_messages
WHERE message_timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND reply_count = 0

ORDER BY item_date DESC;
