CREATE SCHEMA IF NOT EXISTS silver;

-- Unified transactions: POS + E-commerce standardized here
CREATE TABLE IF NOT EXISTS silver.transactions (
  transaction_id STRING,
  sale_ts TIMESTAMP,
  sale_date DATE,
  product_id INT64,
  store_id INT64,                      -- 0 = online
  customer_id INT64,
  quantity INT64,
  sale_amount BIGNUMERIC,
  is_return BOOL,
  source_system STRING,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY sale_date
CLUSTER BY store_id, product_id, customer_id;

-- Customer Profiles SCD Type 2: keep full history of changes
CREATE TABLE IF NOT EXISTS silver.customer_profiles_scd (
  customer_id INT64,
  country STRING,
  registration_date DATE,
  loyalty_tier STRING,
  effective_start TIMESTAMP,
  effective_end TIMESTAMP,
  is_current BOOL,
  record_hash STRING,                 -- detect changes without comparing many columns
  ingestion_timestamp TIMESTAMP
)
PARTITION BY DATE(effective_start)
CLUSTER BY customer_id;

-- Inventory cleaned snapshots
CREATE TABLE IF NOT EXISTS silver.inventory (
  snapshot_date DATE,
  product_id INT64,
  store_id INT64,
  product_name STRING,
  quantity_on_hand INT64,
  price NUMERIC,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY snapshot_date
CLUSTER BY store_id, product_id;
