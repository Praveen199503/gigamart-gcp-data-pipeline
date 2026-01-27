CREATE SCHEMA IF NOT EXISTS giga;

-- Final transactions table
CREATE TABLE IF NOT EXISTS giga.transactions (
  transaction_id STRING,
  sale_date DATE,
  product_id INT64,
  store_id INT64,                      -- 0 = online
  customer_id INT64,
  quantity INT64,
  sale_amount BIGNUMERIC,
  is_return BOOL,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY sale_date
CLUSTER BY product_id, store_id, customer_id;

-- Final customer profiles (current snapshot used by analytics)
CREATE TABLE IF NOT EXISTS giga.customer_profiles (
  customer_id INT64,
  country STRING,
  registration_date DATE,
  loyalty_tier STRING,
  is_active BOOL
)
CLUSTER BY country, loyalty_tier;
