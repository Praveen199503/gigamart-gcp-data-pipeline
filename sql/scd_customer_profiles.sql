--Build a source set with a stable "record_hash" for change detection

WITH src AS (
  SELECT
    customer_id,
    country,
    registration_date,
    loyalty_tier,
    profile_updated_ts AS effective_start,
    TO_HEX(SHA256(CONCAT(
      COALESCE(CAST(customer_id AS STRING), ''),
      '|', COALESCE(country, ''),
      '|', COALESCE(CAST(registration_date AS STRING), ''),
      '|', COALESCE(loyalty_tier, '')
    ))) AS record_hash,
    ingestion_timestamp
  FROM silver.crm_customer_staging
),

--Current records in SCD table
cur AS (
  SELECT *
  FROM silver.customer_profiles_scd
  WHERE is_current = TRUE
)

--Close changed current records
UPDATE silver.customer_profiles_scd t
SET
  effective_end = s.effective_start,
  is_current = FALSE
FROM src s
WHERE t.customer_id = s.customer_id
  AND t.is_current = TRUE
  AND t.record_hash != s.record_hash;

--Insert new records (new customers OR changed customers)
INSERT INTO silver.customer_profiles_scd (
  customer_id,
  country,
  registration_date,
  loyalty_tier,
  effective_start,
  effective_end,
  is_current,
  record_hash,
  ingestion_timestamp
)
SELECT
  s.customer_id,
  s.country,
  s.registration_date,
  s.loyalty_tier,
  s.effective_start,
  NULL AS effective_end,
  TRUE AS is_current,
  s.record_hash,
  s.ingestion_timestamp
FROM src s
LEFT JOIN cur c
  ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL           -- brand new customer
   OR c.record_hash != s.record_hash; -- changed customer