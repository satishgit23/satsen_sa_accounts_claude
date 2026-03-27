-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Gmail Emails
-- MAGIC Parsed emails with open/pending item identification

CREATE OR REFRESH MATERIALIZED VIEW silver_emails
AS
SELECT
  message_id,
  subject,
  from_email,
  to_email,
  CAST(date AS TIMESTAMP) AS email_date,
  snippet,
  is_unread,
  labels,
  -- Identify open/pending items by keywords in subject and snippet
  CASE
    WHEN is_unread = true THEN true
    WHEN lower(subject) RLIKE '(action required|follow.?up|pending|todo|please respond|waiting|urgent|asap|review needed)'
      THEN true
    WHEN lower(snippet) RLIKE '(action required|follow.?up|pending|todo|please respond|waiting|urgent|asap|review needed)'
      THEN true
    ELSE false
  END AS is_open_item,
  -- Extract potential account name from subject/from
  CASE
    WHEN from_email IS NOT NULL THEN split(from_email, '@')[1]
    ELSE NULL
  END AS sender_domain,
  _ingested_at
FROM bronze_gmail_emails
WHERE message_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY _ingested_at DESC) = 1;
