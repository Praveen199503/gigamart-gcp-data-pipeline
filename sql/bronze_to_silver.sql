-- BRONZE -> SILVER
-- Purpose:
-- 1) Parse raw payload (JSON string) from bronze tables
-- 2) Standardize into one unified transactions schema
-- 3) Add ingestion_timestamp so we can dedupe later (latest wins)

-- =========================
-- 4A) POS -> SILVER TRANSACTIONS
-- =========================
-- Assumptions for sample dataset:
-- - POS payload contains transaction_id, store_id, customer_id, amount, timestamp
-- - quantity and product_id may be missing in raw (we default quantity=1, product_id=NULL)
-- - is_return derived from event_type if present, else FALSE

INSERT INTO silver.transactions (
  transaction_id,
  sale_ts,
  sale_date,
  product_id,
  store_id,
  customer_id,
  quantity,
  sale_amount,
  is_return,
  source_system,
  ingestion_timestamp
)
SELECT
  JSON_VALUE(payload, '$.transaction_id') AS transaction_id,
  TIMESTAMP(JSON_VALUE(payload, '$.timestamp')) AS sale_ts,
  DATE(TIMESTAMP(JSON_VALUE(payload, '$.timestamp'))) AS sale_date,

  -- product_id may not exist in POS events; keep NULL unless present
  SAFE_CAST(JSON_VALUE(payload, '$.product_id') AS INT64) AS product_id,

  -- store_id might come like "S001" in sample files; convert if numeric, else NULL
  SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(payload, '$.store_id'), r'(\d+)') AS INT64) AS store_id,

  SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(payload, '$.customer_id'), r'(\d+)') AS INT64) AS customer_id,

  COALESCE(SAFE_CAST(JSON_VALUE(payload, '$.quantity') AS INT64), 1) AS quantity,

  SAFE_CAST(JSON_VALUE(payload, '$.amount') AS BIGNUMERIC) AS sale_amount,

  -- if event_type exists and equals "return", treat as return; else FALSE
  IF(LOWER(JSON_VALUE(payload, '$.event_type')) = 'return', TRUE, FALSE) AS is_return,

  'POS' AS source_system,
  ingestion_timestamp
FROM bronze.pos_raw
WHERE JSON_VALUE(payload, '$.transaction_id') IS NOT NULL;


-- =========================
-- 4B) E-COM -> SILVER TRANSACTIONS
-- =========================
-- Assumptions for sample dataset:
-- - E-commerce uses order_id instead of transaction_id
-- - store_id for online is 0 (per requirement)
-- - total_amount maps to sale_amount

INSERT INTO silver.transactions (
  transaction_id,
  sale_ts,
  sale_date,
  product_id,
  store_id,
  customer_id,
  quantity,
  sale_amount,
  is_return,
  source_system,
  ingestion_timestamp
)
SELECT
  JSON_VALUE(payload, '$.order_id') AS transaction_id,
  TIMESTAMP(JSON_VALUE(payload, '$.timestamp')) AS sale_ts,
  DATE(TIMESTAMP(JSON_VALUE(payload, '$.timestamp'))) AS sale_date,

  SAFE_CAST(JSON_VALUE(payload, '$.product_id') AS INT64) AS product_id,

  0 AS store_id,  -- online sales

  SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(payload, '$.customer_id'), r'(\d+)') AS INT64) AS customer_id,

  COALESCE(SAFE_CAST(JSON_VALUE(payload, '$.quantity') AS INT64), 1) AS quantity,

  SAFE_CAST(JSON_VALUE(payload, '$.total_amount') AS BIGNUMERIC) AS sale_amount,

  IF(LOWER(JSON_VALUE(payload, '$.is_return')) = 'true', TRUE, FALSE) AS is_return,

  'ECOM' AS source_system,
  ingestion_timestamp
FROM bronze.ecom_raw
WHERE JSON_VALUE(payload, '$.order_id') IS NOT NULL;



-- We do NOT overwrite silver.transactions here; we create a view to use downstream.

CREATE OR REPLACE VIEW silver.transactions_latest AS
SELECT * EXCEPT(rn)
FROM (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY ingestion_timestamp DESC
    ) AS rn
  FROM silver.transactions t
)
WHERE rn = 1;
