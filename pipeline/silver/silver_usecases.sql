-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Salesforce Use Cases
-- MAGIC Cleaned use cases joined with account names

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_usecases
AS
SELECT
  uc.Id AS usecase_id,
  uc.Name AS usecase_name,
  uc.Account_Name AS account_name,
  uc.Account_Id AS account_id,
  uc.Stage AS usecase_stage,
  uc.Status AS usecase_status,
  uc.Description AS description,
  uc.Product_Area AS product_area,
  uc.Workload AS workload,
  CAST(uc.CreatedDate AS TIMESTAMP) AS created_date,
  CAST(uc.LastModifiedDate AS TIMESTAMP) AS last_modified_date,
  uc._ingested_at
FROM bronze_salesforce_usecases uc
WHERE uc.Id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY uc.Id ORDER BY uc._ingested_at DESC) = 1;
