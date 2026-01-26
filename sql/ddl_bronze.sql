-- BRONZE LAYER (RAW)
-- Purpose: land all incoming data safely without breaking on schema changes.
-- Keep ingestion_timestamp for audit and late-arriving/dedup logic later.

CREATE SCHEMA IF NOT EXISTS bronze;

-- POS raw: JSON payload as string (streaming)
CREATE TABLE IF NOT EXISTS bronze.pos_raw (
  source_system STRING,                 -- 'POS'
  payload STRING,                       -- raw JSON string
  ingestion_timestamp TIMESTAMP         -- when this row landed
)
PARTITION BY DATE(ingestion_timestamp);

-- E-commerce raw: JSON payload as string (hourly files)
CREATE TABLE IF NOT EXISTS bronze.ecom_raw (
  source_system STRING,                 -- 'ECOM'
  payload STRING,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY DATE(ingestion_timestamp);

-- CRM raw: store each CSV row as JSON string (schema evolves)
CREATE TABLE IF NOT EXISTS bronze.crm_raw (
  source_system STRING,                 -- 'CRM'
  payload STRING,                       -- JSON string representing the row
  file_date DATE,                       -- date inferred from filename
  ingestion_timestamp TIMESTAMP
)
PARTITION BY file_date;

-- ERP inventory raw: store each CSV row as JSON string
CREATE TABLE IF NOT EXISTS bronze.inventory_raw (
  source_system STRING,                 -- 'ERP'
  payload STRING,
  file_date DATE,
  ingestion_timestamp TIMESTAMP
)
PARTITION BY file_date;
