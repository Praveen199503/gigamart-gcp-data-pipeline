# GigaMart GCP Data Pipeline – Design Doc

## 1. Problem Summary
GigaMart operates across multiple countries and collects retail data from multiple systems with different formats and frequencies. The goal is to ingest, cleanse, unify, and load this data into BigQuery to enable near real-time marketing dashboards and daily finance reconciliation, with governance and cost efficiency.

## 2. Data Sources
- POS terminals (in-store): continuous JSON events via Pub/Sub :contentReference
- E-commerce portal: hourly JSON files via Cloud Storage 
- CRM: daily CSV exports via Cloud Storage 
- Supply chain ERP: daily CSV exports via Cloud Storage 

## 3. Proposed GCP Architecture
### Streaming (POS)
- Pub/Sub topic receives POS events.
- A long-running Dataflow streaming job writes to BigQuery Bronze (raw), attaching ingestion_timestamp for deduplication.

### Batch (E-commerce / CRM / ERP)
- Files land in GCS landing paths.
- Cloud Composer (or Workflows) triggers batch Dataflow / BigQuery load jobs into BigQuery Bronze.

### Storage & Modeling (BigQuery)
- Bronze: append-only raw landing tables (schema-flexible payload storage).
- Silver: cleansed, standardized tables and SCD history table.
- Gold: analytics tables aligned to business queries (`giga.transactions`, `giga.customer_profiles`).

## 4. Data Modeling Approach (Bronze/Silver/Gold)
### Bronze (Raw)
Purpose: ensure ingestion never breaks (especially with CRM schema evolution), and preserve raw data for replay/audit.

Tables:
- bronze.pos_raw, bronze.ecom_raw, bronze.crm_raw, bronze.inventory_raw

### Silver (Standardized)
- silver.transactions: unified POS + ecom transactions schema
- silver.transactions_latest: deduped latest view using ingestion_timestamp
- silver.customer_profiles_scd: SCD Type 2 customer history
- silver.inventory: cleaned daily inventory snapshot

### Gold (Analytics)
- giga.transactions: partitioned & clustered analytics table
- giga.customer_profiles: current snapshot + is_active derived from transactions

## 5. How Key Challenges Are Addressed
1. Different frequencies and formats  
   - Streaming for POS; batch ingestion for file-based sources; unified Silver schema.

2. Existing batch ingestion takes >12 hours  
   - Dataflow parallelism + BigQuery SQL transformations reduce processing time and remove manual batch steps.

3. Duplicate and delayed transactions (late uploads)  
   - Store ingestion_timestamp; dedupe by keeping latest record per transaction_id. 

4. CRM schema mismatches due to new optional fields  
   - Bronze stores schema-flexible payload; Silver staging uses COALESCE across possible field names. 

5. Customer profiles change over time and require historical tracking  
   - Implement SCD Type 2 in silver.customer_profiles_scd with effective start/end and is_current. 

6. Data inconsistencies causing incorrect totals  
   - Centralized transformations in Silver + data quality checks before publishing Gold.

7. Marketing needs real-time, finance needs daily  
   - Streaming ingestion supports near real-time availability; daily DAG refreshes Gold and runs reconciliation checks.

8. Pipelines should self-monitor and recover  
   - Orchestration includes retries and validation gating; monitoring/alerts capture failures.

## 6. Incremental Loading Strategy
- Gold transactions are maintained incrementally using MERGE by transaction_id. Late-arriving updates are captured using ingestion_timestamp.
- A small lookback window (e.g., last 3 days) ensures delayed uploads are reprocessed safely without requiring an external control table.

## 7. Assumptions
- Some fields may be missing from raw sample payloads; defaults are applied conservatively (e.g., quantity defaults to 1, product_id may be NULL).
- Deployment is not executed because the assessment does not provide a GCP project/credentials; SQL and orchestration are written to be directly runnable in a standard GCP environment with minimal configuration.
