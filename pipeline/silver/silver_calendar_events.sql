-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Google Calendar Events
-- MAGIC Standardized events with attendees and account linkage

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_calendar_events
AS
SELECT
  event_id,
  summary AS event_title,
  description AS agenda,
  CAST(start_time AS TIMESTAMP) AS start_time,
  CAST(end_time AS TIMESTAMP) AS end_time,
  attendees,
  organizer,
  location,
  status AS event_status,
  CAST(created AS TIMESTAMP) AS created_date,
  CAST(updated AS TIMESTAMP) AS updated_date,
  CAST(start_time AS DATE) AS event_date,
  _ingested_at
FROM bronze_google_calendar
WHERE event_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC) = 1;
