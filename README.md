# Batch Analytics Warehouse Pipeline

Scheduled ETL pipeline in Cloud Composer (Airflow) to merge ERP, CRM, and app logs into analytics marts with DQ checks and rerun-safe logic.

## Stack
Cloud Composer (Airflow), BigQuery SQL, Python

## Files
- `dags/batch_analytics_warehouse.py`: Airflow DAG for end-to-end batch ETL
- `sql/create_raw_tables.sql`: raw tables expected by the DAG
- `sample-data/*.csv`: test source files for `raw` loads
- `sample-data/README.md`: exact load commands + trigger config

## DAG Behavior
- Schedule: daily at `02:00`
- Loads and merges staging data from raw sources
- Builds `mart.daily_business_kpis`
- Runs data quality checks:
  - records present
  - no duplicate orders
  - null-rate threshold

## Deploy
1. Set `PROJECT_ID` and datasets in the DAG file.
2. Upload DAG to Composer:
   ```bash
   gcloud composer environments storage dags import \
     --environment YOUR_COMPOSER_ENV \
     --location YOUR_REGION \
     --source dags/batch_analytics_warehouse.py
   ```

## Rerun for a Date
Trigger DAG with config:
```json
{"process_date":"2026-02-20"}
```

This is rerun-safe because staging uses `MERGE` and mart uses date-based replace logic.
