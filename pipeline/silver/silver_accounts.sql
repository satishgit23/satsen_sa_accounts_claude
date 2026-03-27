-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: Salesforce Accounts
-- MAGIC Cleaned and standardized account data

CREATE OR REFRESH MATERIALIZED VIEW silver_accounts
AS
SELECT
  Id AS account_id,
  Name AS account_name,
  Industry AS industry,
  Type AS account_type,
  BillingCity AS billing_city,
  BillingState AS billing_state,
  BillingCountry AS billing_country,
  Owner_Name AS owner_name,
  CAST(CreatedDate AS TIMESTAMP) AS created_date,
  CAST(LastModifiedDate AS TIMESTAMP) AS last_modified_date,
  _ingested_at
FROM bronze_salesforce_accounts
WHERE Id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY _ingested_at DESC) = 1;
