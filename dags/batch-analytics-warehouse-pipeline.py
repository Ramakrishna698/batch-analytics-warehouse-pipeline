from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)

PROJECT_ID = "YOUR_PROJECT"
LOCATION = "US"

RAW_DATASET = "raw"
STG_DATASET = "staging"
MART_DATASET = "mart"

# Rerun logic: trigger DAG with {"process_date":"YYYY-MM-DD"}.
# If not provided, Airflow uses ds (scheduled date).
PROCESS_DATE = "{{ dag_run.conf.get('process_date', ds) }}"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_analytics_warehouse",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",  # daily 2 AM
    catchup=False,
    default_args=default_args,
    tags=["composer", "etl", "warehouse"],
) as dag:

    create_schemas = BigQueryInsertJobOperator(
        task_id="create_schemas",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{STG_DATASET}`;
                CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{MART_DATASET}`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # upsert_erp_orders = BigQueryInsertJobOperator(
    #     task_id="upsert_erp_orders",
    #     location=LOCATION,
    #     configuration={
    #         "query": {
    #             "query": f"""
    #             CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{STG_DATASET}.erp_orders` (
    #               order_id STRING,
    #               customer_id STRING,
    #               order_ts TIMESTAMP,
    #               order_date DATE,
    #               order_amount NUMERIC,
    #               status STRING,
    #               etl_loaded_at TIMESTAMP
    #             )
    #             PARTITION BY order_date
    #             CLUSTER BY customer_id;

    #             MERGE `{PROJECT_ID}.{STG_DATASET}.erp_orders` T
    #             USING (
    #               SELECT
    #                 CAST(order_id AS STRING) AS order_id,
    #                 CAST(customer_id AS STRING) AS customer_id,
    #                 TIMESTAMP(order_ts) AS order_ts,
    #                 DATE(order_ts) AS order_date,
    #                 CAST(order_amount AS NUMERIC) AS order_amount,
    #                 CAST(status AS STRING) AS status,
    #                 CURRENT_TIMESTAMP() AS etl_loaded_at
    #               FROM `{PROJECT_ID}.{RAW_DATASET}.erp_orders`
    #               WHERE DATE(_ingest_ts) = DATE('{PROCESS_DATE}')
    #             ) S
    #             ON T.order_id = S.order_id
    #             WHEN MATCHED THEN UPDATE SET
    #               customer_id = S.customer_id,
    #               order_ts = S.order_ts,
    #               order_date = S.order_date,
    #               order_amount = S.order_amount,
    #               status = S.status,
    #               etl_loaded_at = S.etl_loaded_at
    #             WHEN NOT MATCHED THEN INSERT (
    #               order_id, customer_id, order_ts, order_date, order_amount, status, etl_loaded_at
    #             ) VALUES (
    #               S.order_id, S.customer_id, S.order_ts, S.order_date, S.order_amount, S.status, S.etl_loaded_at
    #             );
    #             """,
    #             "useLegacySql": False,
    #         }
    #     },
    # )
