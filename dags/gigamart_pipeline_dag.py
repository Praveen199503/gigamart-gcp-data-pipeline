"""
GigaMart GCP Pipeline Orchestration (Cloud Composer / Airflow)

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gigamart_end_to_end_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingest -> Bronze -> Silver -> Gold (MERGE) -> DQ checks",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gigamart", "gcp", "bigquery", "dataflow"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------
    # Ingestion
    # -----------------------------
    # In a real Composer environment, these would trigger:
    # - Dataflow streaming job for POS (usually long-running, monitored separately)
    # - Dataflow batch jobs / BigQuery load jobs for GCS files (ecom/crm/erp)
    ingest_pos_streaming = EmptyOperator(task_id="ingest_pos_streaming_monitored")

    ingest_ecom_batch = EmptyOperator(task_id="ingest_ecom_batch_from_gcs")
    ingest_crm_batch = EmptyOperator(task_id="ingest_crm_batch_from_gcs")
    ingest_erp_batch = EmptyOperator(task_id="ingest_erp_inventory_batch_from_gcs")

    ingestion_done = EmptyOperator(task_id="ingestion_done")

    # -----------------------------
    # Transformations (SQL scripts executed as BigQuery jobs in production)
    # can Replace BashOperator with BigQueryInsertJobOperator in real deployment.
    # -----------------------------
    bronze_to_silver_tx = BashOperator(
        task_id="bronze_to_silver_transactions",
        bash_command="echo 'Run sql/bronze_to_silver.sql in BigQuery'",
    )

    crm_staging = BashOperator(
        task_id="crm_to_silver_staging",
        bash_command="echo 'Run sql/crm_to_silver_staging.sql in BigQuery'",
    )

    scd_customers = BashOperator(
        task_id="scd_customer_profiles",
        bash_command="echo 'Run sql/scd_customer_profiles.sql in BigQuery'",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_incremental_merge",
        bash_command="echo 'Run sql/silver_to_gold.sql in BigQuery'",
    )

    dq_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command="echo 'Run validation/dq_checks.sql in BigQuery and fail DAG if any FAIL'",
    )

    end = EmptyOperator(task_id="end")

    # -----------------------------
    # Dependencies
    # -----------------------------
    start >> [ingest_pos_streaming, ingest_ecom_batch, ingest_crm_batch, ingest_erp_batch] >> ingestion_done

    ingestion_done >> bronze_to_silver_tx
    ingestion_done >> crm_staging >> scd_customers

    [bronze_to_silver_tx, scd_customers] >> silver_to_gold >> dq_checks >> end
