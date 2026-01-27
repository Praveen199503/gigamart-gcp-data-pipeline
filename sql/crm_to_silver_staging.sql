--CRM BRONZE -> STAGING VIEW (schema-evolution friendly)

CREATE OR REPLACE VIEW silver.crm_customer_staging AS
SELECT
  SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(payload, '$.customer_id'), r'(\d+)') AS INT64) AS customer_id,

  JSON_VALUE(payload, '$.country') AS country,

  SAFE_CAST(JSON_VALUE(payload, '$.registration_date') AS DATE) AS registration_date,

  COALESCE(
    JSON_VALUE(payload, '$.membership_tier'),
    JSON_VALUE(payload, '$.loyalty_tier'),
    JSON_VALUE(payload, '$.loyalty_status')
  ) AS loyalty_tier,

  COALESCE(
    TIMESTAMP(JSON_VALUE(payload, '$.updated_at')),
    ingestion_timestamp
  ) AS profile_updated_ts,

  ingestion_timestamp
FROM bronze.crm_raw
WHERE JSON_VALUE(payload, '$.customer_id') IS NOT NULL;