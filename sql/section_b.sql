-- 1) Top 5 selling products by total sale_amount in 2024
-- Include product_id, total quantity sold, total sale_amount.
SELECT
  product_id,
  SUM(quantity) AS total_quantity_sold,
  SUM(sale_amount) AS total_sale_amount
FROM giga.transactions
WHERE sale_date >= DATE '2024-01-01'
  AND sale_date <  DATE '2025-01-01'
  AND is_return = FALSE
GROUP BY product_id
ORDER BY total_sale_amount DESC
LIMIT 5;

-- 2) Month-over-Month (MoM) % growth in total sale_amount for 2024
-- Output: month (as DATE like '2024-01-01'), total_sale_amount, percentage change vs prev month.
-- Assume Jan 2024 has 0% growth.
WITH monthly AS (
  SELECT
    DATE_TRUNC(sale_date, MONTH) AS month_start,
    SUM(sale_amount) AS total_sale_amount
  FROM giga.transactions
  WHERE sale_date >= DATE '2024-01-01'
    AND sale_date <  DATE '2025-01-01'
    AND is_return = FALSE
  GROUP BY month_start
),
with_prev AS (
  SELECT
    month_start,
    total_sale_amount,
    LAG(total_sale_amount) OVER (ORDER BY month_start) AS prev_month_amount
  FROM monthly
)
SELECT
  month_start AS month,
  total_sale_amount,
  CASE
    WHEN prev_month_amount IS NULL THEN 0
    WHEN prev_month_amount = 0 THEN 0
    ELSE ROUND( ( (total_sale_amount - prev_month_amount) / prev_month_amount ) * 100, 2)
  END AS mom_growth_percent
FROM with_prev
ORDER BY month;

-- 3) Total number of distinct active customers who are 'Gold'
-- and have made a purchase of a non-return item with sale_amount > 100
SELECT
  COUNT(DISTINCT t.customer_id) AS distinct_active_gold_customers
FROM giga.transactions t
JOIN giga.customer_profiles c
  ON t.customer_id = c.customer_id
WHERE c.is_active = TRUE
  AND c.loyalty_tier = 'Gold'
  AND t.is_return = FALSE
  AND t.sale_amount > 100;

-- 4) Retrieve only the latest, non-duplicate version of every transaction
-- based on the most recent ingestion_timestamp.
SELECT * EXCEPT(rn)
FROM (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY ingestion_timestamp DESC
    ) AS rn
  FROM giga.transactions t
)
WHERE rn = 1;

-- 5) Customer Lifetime Value (CLV) = total net sales (sales - returns)
-- since customer registration. Output: customer_id, country, clv_amount ranked desc.
WITH tx AS (
  SELECT
    t.customer_id,
    -- Net sale: add sales, subtract returns
    SUM(CASE WHEN t.is_return THEN -t.sale_amount ELSE t.sale_amount END) AS clv_amount
  FROM giga.transactions t
  GROUP BY t.customer_id
)
SELECT
  c.customer_id,
  c.country,
  tx.clv_amount
FROM tx
JOIN giga.customer_profiles c
  ON tx.customer_id = c.customer_id
ORDER BY tx.clv_amount DESC;