from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)

PROJECT_ID = "practice-project-488818"
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
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_analytics_warehouse",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  # daily 2 AM
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

    upsert_erp_orders = BigQueryInsertJobOperator(
        task_id="upsert_erp_orders",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{STG_DATASET}.erp_orders` (
                  order_id STRING,
                  customer_id STRING,
                  order_ts TIMESTAMP,
                  order_date DATE,
                  order_amount NUMERIC,
                  status STRING,
                  etl_loaded_at TIMESTAMP
                )
                PARTITION BY order_date
                CLUSTER BY customer_id;

                MERGE `{PROJECT_ID}.{STG_DATASET}.erp_orders` T
                USING (
                  SELECT
                    CAST(order_id AS STRING) AS order_id,
                    CAST(customer_id AS STRING) AS customer_id,
                    TIMESTAMP(order_ts) AS order_ts,
                    DATE(order_ts) AS order_date,
                    CAST(order_amount AS NUMERIC) AS order_amount,
                    CAST(status AS STRING) AS status,
                    CURRENT_TIMESTAMP() AS etl_loaded_at
                  FROM `{PROJECT_ID}.{RAW_DATASET}.erp_orders`
                  WHERE DATE(_ingest_ts) = DATE('{PROCESS_DATE}')
                ) S
                ON T.order_id = S.order_id
                WHEN MATCHED THEN UPDATE SET
                  customer_id = S.customer_id,
                  order_ts = S.order_ts,
                  order_date = S.order_date,
                  order_amount = S.order_amount,
                  status = S.status,
                  etl_loaded_at = S.etl_loaded_at
                WHEN NOT MATCHED THEN INSERT (
                  order_id, customer_id, order_ts, order_date, order_amount, status, etl_loaded_at
                ) VALUES (
                  S.order_id, S.customer_id, S.order_ts, S.order_date, S.order_amount, S.status, S.etl_loaded_at
                );
                """,
                "useLegacySql": False,
            }
        },
    )
    upsert_crm_customers = BigQueryInsertJobOperator(
        task_id="upsert_crm_customers",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{STG_DATASET}.crm_customers` (
                  customer_id STRING,
                  segment STRING,
                  region STRING,
                  updated_at TIMESTAMP,
                  etl_loaded_at TIMESTAMP
                )
                PARTITION BY DATE(etl_loaded_at)
                CLUSTER BY customer_id;

                MERGE `{PROJECT_ID}.{STG_DATASET}.crm_customers` T
                USING (
                  SELECT customer_id, segment, region, updated_at, CURRENT_TIMESTAMP() AS etl_loaded_at
                  FROM (
                    SELECT
                      CAST(customer_id AS STRING) AS customer_id,
                      CAST(segment AS STRING) AS segment,
                      CAST(region AS STRING) AS region,
                      TIMESTAMP(updated_at) AS updated_at,
                      ROW_NUMBER() OVER (
                        PARTITION BY customer_id
                        ORDER BY TIMESTAMP(updated_at) DESC
                      ) AS rn
                    FROM `{PROJECT_ID}.{RAW_DATASET}.crm_customers`
                    WHERE DATE(_ingest_ts) <= DATE('{PROCESS_DATE}')
                  )
                  WHERE rn = 1
                ) S
                ON T.customer_id = S.customer_id
                WHEN MATCHED THEN UPDATE SET
                  segment = S.segment,
                  region = S.region,
                  updated_at = S.updated_at,
                  etl_loaded_at = S.etl_loaded_at
                WHEN NOT MATCHED THEN INSERT (
                  customer_id, segment, region, updated_at, etl_loaded_at
                ) VALUES (
                  S.customer_id, S.segment, S.region, S.updated_at, S.etl_loaded_at
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    upsert_app_events = BigQueryInsertJobOperator(
        task_id="upsert_app_events",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{STG_DATASET}.app_events` (
                  event_id STRING,
                  user_id STRING,
                  event_name STRING,
                  event_ts TIMESTAMP,
                  event_date DATE,
                  etl_loaded_at TIMESTAMP
                )
                PARTITION BY event_date
                CLUSTER BY user_id, event_name;

                MERGE `{PROJECT_ID}.{STG_DATASET}.app_events` T
                USING (
                  SELECT
                    CAST(event_id AS STRING) AS event_id,
                    CAST(user_id AS STRING) AS user_id,
                    CAST(event_name AS STRING) AS event_name,
                    TIMESTAMP(event_ts) AS event_ts,
                    DATE(event_ts) AS event_date,
                    CURRENT_TIMESTAMP() AS etl_loaded_at
                  FROM `{PROJECT_ID}.{RAW_DATASET}.app_events`
                  WHERE DATE(_ingest_ts) = DATE('{PROCESS_DATE}')
                ) S
                ON T.event_id = S.event_id
                WHEN MATCHED THEN UPDATE SET
                  user_id = S.user_id,
                  event_name = S.event_name,
                  event_ts = S.event_ts,
                  event_date = S.event_date,
                  etl_loaded_at = S.etl_loaded_at
                WHEN NOT MATCHED THEN INSERT (
                  event_id, user_id, event_name, event_ts, event_date, etl_loaded_at
                ) VALUES (
                  S.event_id, S.user_id, S.event_name, S.event_ts, S.event_date, S.etl_loaded_at
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    build_daily_mart = BigQueryInsertJobOperator(
        task_id="build_daily_mart",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{MART_DATASET}.daily_business_kpis` (
                  metric_date DATE,
                  orders INT64,
                  revenue NUMERIC,
                  active_customers INT64,
                  active_app_users INT64,
                  run_ts TIMESTAMP
                )
                PARTITION BY metric_date;

                DELETE FROM `{PROJECT_ID}.{MART_DATASET}.daily_business_kpis`
                WHERE metric_date = DATE('{PROCESS_DATE}');

                INSERT INTO `{PROJECT_ID}.{MART_DATASET}.daily_business_kpis`
                SELECT
                  DATE('{PROCESS_DATE}') AS metric_date,
                  COUNT(DISTINCT o.order_id) AS orders,
                  SUM(o.order_amount) AS revenue,
                  COUNT(DISTINCT o.customer_id) AS active_customers,
                  COUNT(DISTINCT a.user_id) AS active_app_users,
                  CURRENT_TIMESTAMP() AS run_ts
                FROM `{PROJECT_ID}.{STG_DATASET}.erp_orders` o
                LEFT JOIN `{PROJECT_ID}.{STG_DATASET}.crm_customers` c
                  ON o.customer_id = c.customer_id
                LEFT JOIN `{PROJECT_ID}.{STG_DATASET}.app_events` a
                  ON o.customer_id = a.user_id
                 AND a.event_date = DATE('{PROCESS_DATE}')
                WHERE o.order_date = DATE('{PROCESS_DATE}');
                """,
                "useLegacySql": False,
            }
        },
    )

    dq_orders_present = BigQueryCheckOperator(
        task_id="dq_orders_present",
        use_legacy_sql=False,
        location=LOCATION,
        sql=f"""
        SELECT COUNT(*) > 0
        FROM `{PROJECT_ID}.{STG_DATASET}.erp_orders`
        WHERE order_date = DATE('{PROCESS_DATE}')
        """,
    )

    dq_no_duplicate_orders = BigQueryCheckOperator(
        task_id="dq_no_duplicate_orders",
        use_legacy_sql=False,
        location=LOCATION,
        sql=f"""
        SELECT COUNT(*) = 0
        FROM (
          SELECT order_id, COUNT(*) AS c
          FROM `{PROJECT_ID}.{STG_DATASET}.erp_orders`
          WHERE order_date = DATE('{PROCESS_DATE}')
          GROUP BY order_id
          HAVING c > 1
        )
        """,
    )

    dq_null_customer_rate = BigQueryCheckOperator(
        task_id="dq_null_customer_rate",
        use_legacy_sql=False,
        location=LOCATION,
        sql=f"""
        SELECT COALESCE(
          SAFE_DIVIDE(COUNTIF(customer_id IS NULL), COUNT(*)),
          0
        ) < 0.02
        FROM `{PROJECT_ID}.{STG_DATASET}.erp_orders`
        WHERE order_date = DATE('{PROCESS_DATE}')
        """,
    )

    create_schemas >> [upsert_erp_orders, upsert_crm_customers, upsert_app_events]
    [upsert_erp_orders, upsert_crm_customers, upsert_app_events] >> build_daily_mart
    build_daily_mart >> [dq_orders_present, dq_no_duplicate_orders, dq_null_customer_rate]

