# GigaMart GCP Data Pipeline

## Overview
This repository contains an end-to-end GCP data pipeline design and implementation artifacts for ingesting POS, e-commerce, CRM, and ERP inventory data into BigQuery using a Bronze/Silver/Gold approach.

## Repository Structure
- docs/ : architecture diagram and design documents
- sql/ : DDL and transformation SQL (Bronze >> Silver >> Gold)
- validation/ : data quality checks
- dags/ : Cloud Composer (Airflow) orchestration skeleton

## Key Deliverables
- Architecture diagram: docs/architecture_diagram.png
- Design doc: docs/design_doc.md
- Optimization notes: docs/optimization_notes.md
- SQL:
  - sql/ddl_bronze.sql
  - sql/ddl_silver.sql
  - sql/ddl_gold.sql
  - sql/bronze_to_silver.sql
  - sql/crm_to_silver_staging.sql
  - sql/scd_customer_profiles.sql
  - sql/silver_to_gold.sql (incremental MERGE)
  - sql/section_b.sql
- Data quality checks: validation/dq_checks.sql
- Orchestration: dags/gigamart_pipeline_dag.py

## Execution Note
All SQL and orchestration artifacts are written to be directly executable in a standard GCP environment with minimal configuration.

## How it Works (High Level)
1. Ingest raw data into BigQuery Bronze (POS streaming + file-based batch loads).
2. Transform into Silver (standardized transactions, SCD Type 2 customers, inventory).
3. Incrementally MERGE into Gold tables used for analytics.
4. Run data quality checks; alert on failures.

## Section B
Answers to the 5 SQL questions are in: sql/section_b.sql
