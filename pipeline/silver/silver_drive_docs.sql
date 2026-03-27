-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Google Drive Documents
-- MAGIC Document summaries and action items by customer

CREATE OR REFRESH MATERIALIZED VIEW silver_drive_docs
AS
SELECT
  file_id,
  file_name,
  folder_name AS customer_folder,
  mime_type,
  CAST(modified_date AS TIMESTAMP) AS modified_date,
  CAST(created_date AS TIMESTAMP) AS created_date,
  content,
  SUBSTRING(content, 1, 500) AS summary,
  _ingested_at
FROM bronze_google_drive_docs
WHERE file_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY file_id ORDER BY _ingested_at DESC) = 1;
