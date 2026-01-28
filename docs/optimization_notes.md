# Optimization Notes (Cost & Performance)

## BigQuery
- Partition giga.transactions by sale_date to reduce scan cost.
- Cluster by product_id, store_id, customer_id to accelerate common filters/joins.
- Prefer incremental MERGE into giga.transactions to avoid full reloads.

## Dataflow
- Use streaming Dataflow for POS to support near real-time metrics.
- Enable autoscaling for batch pipelines.
- Write to BigQuery using Storage Write API for higher throughput (where applicable).

## Late-arriving & duplicates
- Use ingestion_timestamp and “latest wins” logic for deduplication.
- Use a lookback window (e.g., 3 days) to capture delayed uploads without a control table.

## Orchestration / Reliability
- Separate long-running streaming jobs from daily batch orchestration.
- Run DQ checks as a gating step; alert on failures.
- Implement retries with exponential backoff for transient failures.
