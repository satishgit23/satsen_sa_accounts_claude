-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Daily Activity Log
-- MAGIC Time-ordered log of all activities across all sources

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_activity_log
AS

-- Emails
SELECT
  'Email' AS activity_type,
  email_date AS activity_timestamp,
  CAST(email_date AS DATE) AS activity_date,
  subject AS activity_summary,
  from_email AS contact,
  sender_domain AS account_hint,
  CASE WHEN is_unread THEN 'Unread' ELSE 'Read' END AS status
FROM silver_emails

UNION ALL

-- Calendar events
SELECT
  'Meeting' AS activity_type,
  start_time AS activity_timestamp,
  event_date AS activity_date,
  event_title AS activity_summary,
  organizer AS contact,
  NULL AS account_hint,
  event_status AS status
FROM silver_calendar_events

UNION ALL

-- Slack messages
SELECT
  'Slack' AS activity_type,
  message_timestamp AS activity_timestamp,
  CAST(message_timestamp AS DATE) AS activity_date,
  CONCAT('[', channel_name, '] ', SUBSTRING(message_text, 1, 200)) AS activity_summary,
  user_name AS contact,
  derived_account_name AS account_hint,
  'Sent' AS status
FROM silver_slack_messages

UNION ALL

-- Drive doc updates
SELECT
  'Document' AS activity_type,
  modified_date AS activity_timestamp,
  CAST(modified_date AS DATE) AS activity_date,
  CONCAT('Updated: ', file_name) AS activity_summary,
  NULL AS contact,
  customer_folder AS account_hint,
  'Updated' AS status
FROM silver_drive_docs

ORDER BY activity_timestamp DESC;
