-- DATA QUALITY CHECKS
-- Output format: each check returns a row with check_name, status, and issue_count.
-- These can be run as a BigQuery job in Composer/Workflows.

WITH
-- -------------------------
-- Check 1: transaction_id must not be NULL
-- -------------------------
c1 AS (
  SELECT
    'transactions_transaction_id_not_null' AS check_name,
    COUNTIF(transaction_id IS NULL) AS issue_count
  FROM giga.transactions
),

-- -------------------------
-- Check 2: customer_id must not be NULL
-- -------------------------
c2 AS (
  SELECT
    'transactions_customer_id_not_null' AS check_name,
    COUNTIF(customer_id IS NULL) AS issue_count
  FROM giga.transactions
),

-- -------------------------
-- Check 3: sale_amount must be non-negative
-- -------------------------
c3 AS (
  SELECT
    'transactions_sale_amount_non_negative' AS check_name,
    COUNTIF(sale_amount < 0) AS issue_count
  FROM giga.transactions
),

-- -------------------------
-- Check 4: quantity must be positive (for non-return sales)
-- -------------------------
c4 AS (
  SELECT
    'transactions_quantity_positive' AS check_name,
    COUNTIF(is_return = FALSE AND (quantity IS NULL OR quantity <= 0)) AS issue_count
  FROM giga.transactions
),

-- -------------------------
-- Check 5: duplicates by transaction_id (should be 0 if pipeline is correct)
-- -------------------------
c5 AS (
  SELECT
    'transactions_no_duplicate_transaction_id' AS check_name,
    COUNTIF(cnt > 1) AS issue_count
  FROM (
    SELECT transaction_id, COUNT(*) AS cnt
    FROM giga.transactions
    GROUP BY transaction_id
  )
),

-- -------------------------
-- Check 6: loyalty_tier must be one of allowed values (or NULL if missing)
-- -------------------------
c6 AS (
  SELECT
    'customer_profiles_loyalty_tier_valid' AS check_name,
    COUNTIF(loyalty_tier IS NOT NULL AND loyalty_tier NOT IN ('Gold', 'Silver', 'Bronze')) AS issue_count
  FROM giga.customer_profiles
),


SELECT
  check_name,
  IF(issue_count = 0, 'PASS', 'FAIL') AS status,
  issue_count
FROM (
  SELECT * FROM c1
  UNION ALL SELECT * FROM c2
  UNION ALL SELECT * FROM c3
  UNION ALL SELECT * FROM c4
  UNION ALL SELECT * FROM c5
  UNION ALL SELECT * FROM c6
)
ORDER BY status DESC, issue_count DESC, check_name;
