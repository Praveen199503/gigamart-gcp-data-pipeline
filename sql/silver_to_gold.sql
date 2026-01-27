--SILVER -> GOLD

--Build giga.transactions from latest deduped silver view

--Incremental MERGE for giga.transactions
MERGE giga.transactions T
USING (
  SELECT *
  FROM silver.transactions_latest
  WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
) S
ON T.transaction_id = S.transaction_id

WHEN MATCHED
  AND S.ingestion_timestamp > T.ingestion_timestamp THEN
  UPDATE SET
    sale_date = S.sale_date,
    product_id = S.product_id,
    store_id = S.store_id,
    customer_id = S.customer_id,
    quantity = S.quantity,
    sale_amount = S.sale_amount,
    is_return = S.is_return,
    ingestion_timestamp = S.ingestion_timestamp

WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
    S.transaction_id,
    S.sale_date,
    S.product_id,
    S.store_id,
    S.customer_id,
    S.quantity,
    S.sale_amount,
    S.is_return,
    S.ingestion_timestamp
  );

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
