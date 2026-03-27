-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: Account Activity Summary
-- MAGIC Aggregated activity per account across all data sources

CREATE OR REFRESH MATERIALIZED VIEW gold_account_activity_summary
AS
WITH account_base AS (
  SELECT account_id, account_name, industry, account_type, owner_name
  FROM silver_accounts
),

usecase_stats AS (
  SELECT
    account_id,
    COUNT(*) AS total_usecases,
    SUM(CASE WHEN usecase_stage IN ('U1', 'U2', 'U3') THEN 1 ELSE 0 END) AS active_usecases,
    SUM(CASE WHEN usecase_stage IN ('U5', 'U6') THEN 1 ELSE 0 END) AS live_usecases,
    MAX(last_modified_date) AS last_usecase_update
  FROM silver_usecases
  GROUP BY account_id
),

email_stats AS (
  SELECT
    sender_domain,
    COUNT(*) AS total_emails,
    SUM(CASE WHEN is_unread THEN 1 ELSE 0 END) AS unread_emails,
    SUM(CASE WHEN is_open_item THEN 1 ELSE 0 END) AS open_email_items,
    MAX(email_date) AS last_email_date
  FROM silver_emails
  GROUP BY sender_domain
),

calendar_stats AS (
  SELECT
    event_title,
    COUNT(*) AS total_meetings,
    MAX(start_time) AS last_meeting_date,
    MIN(CASE WHEN start_time > current_timestamp() THEN start_time END) AS next_meeting_date
  FROM silver_calendar_events
  GROUP BY event_title
),

slack_stats AS (
  SELECT
    derived_account_name,
    COUNT(*) AS total_slack_messages,
    MAX(message_timestamp) AS last_slack_activity
  FROM silver_slack_messages
  GROUP BY derived_account_name
),

drive_stats AS (
  SELECT
    customer_folder,
    COUNT(*) AS total_docs,
    MAX(modified_date) AS last_doc_update
  FROM silver_drive_docs
  GROUP BY customer_folder
)

SELECT
  a.account_id,
  a.account_name,
  a.industry,
  a.account_type,
  a.owner_name,
  -- Use Case metrics
  COALESCE(uc.total_usecases, 0) AS total_usecases,
  COALESCE(uc.active_usecases, 0) AS active_usecases,
  COALESCE(uc.live_usecases, 0) AS live_usecases,
  uc.last_usecase_update,
  -- Email metrics
  COALESCE(e.total_emails, 0) AS total_emails,
  COALESCE(e.unread_emails, 0) AS unread_emails,
  COALESCE(e.open_email_items, 0) AS open_email_items,
  e.last_email_date,
  -- Slack metrics
  COALESCE(s.total_slack_messages, 0) AS total_slack_messages,
  s.last_slack_activity,
  -- Drive metrics
  COALESCE(d.total_docs, 0) AS total_docs,
  d.last_doc_update,
  -- Engagement health
  DATEDIFF(current_date(), GREATEST(
    COALESCE(e.last_email_date, '1970-01-01'),
    COALESCE(s.last_slack_activity, '1970-01-01'),
    COALESCE(uc.last_usecase_update, '1970-01-01')
  )) AS days_since_last_engagement,
  current_timestamp() AS _refreshed_at
FROM account_base a
LEFT JOIN usecase_stats uc ON a.account_id = uc.account_id
LEFT JOIN email_stats e ON lower(a.account_name) LIKE CONCAT('%', lower(e.sender_domain), '%')
LEFT JOIN slack_stats s ON lower(a.account_name) LIKE CONCAT('%', lower(s.derived_account_name), '%')
LEFT JOIN drive_stats d ON lower(a.account_name) LIKE CONCAT('%', lower(d.customer_folder), '%');
