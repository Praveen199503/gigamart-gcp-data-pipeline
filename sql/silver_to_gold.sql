--SILVER -> GOLD

--Build giga.transactions from latest deduped silver view

--Clear and reload
TRUNCATE TABLE giga.transactions;

INSERT INTO giga.transactions (
  transaction_id,
  sale_date,
  product_id,
  store_id,
  customer_id,
  quantity,
  sale_amount,
  is_return,
  ingestion_timestamp
)
SELECT
  transaction_id,
  sale_date,
  product_id,
  store_id,
  customer_id,
  quantity,
  sale_amount,
  is_return,
  ingestion_timestamp
FROM silver.transactions_latest;

--Build giga.customer_profiles (current snapshot)

TRUNCATE TABLE giga.customer_profiles;

INSERT INTO giga.customer_profiles (
  customer_id,
  country,
  registration_date,
  loyalty_tier,
  is_active
)
WITH last_purchase AS (
  SELECT
    customer_id,
    MAX(sale_date) AS last_purchase_date
  FROM giga.transactions
  WHERE is_return = FALSE
  GROUP BY customer_id
)
SELECT
  s.customer_id,
  s.country,
  s.registration_date,
  s.loyalty_tier,
  IF(
    lp.last_purchase_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH),
    TRUE,
    FALSE
  ) AS is_active  --customer made a purchase (non-return) in last 6 months.S
FROM silver.customer_profiles_scd s
LEFT JOIN last_purchase lp
  ON s.customer_id = lp.customer_id
WHERE s.is_current = TRUE;
