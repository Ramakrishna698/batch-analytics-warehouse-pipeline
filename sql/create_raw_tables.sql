-- Create source dataset used by the DAG
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.raw`;

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.raw.erp_orders` (
  order_id STRING,
  customer_id STRING,
  order_ts TIMESTAMP,
  order_amount NUMERIC,
  status STRING,
  _ingest_ts TIMESTAMP
)
PARTITION BY DATE(_ingest_ts)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.raw.crm_customers` (
  customer_id STRING,
  segment STRING,
  region STRING,
  updated_at TIMESTAMP,
  _ingest_ts TIMESTAMP
)
PARTITION BY DATE(_ingest_ts)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.raw.app_events` (
  event_id STRING,
  user_id STRING,
  event_name STRING,
  event_ts TIMESTAMP,
  _ingest_ts TIMESTAMP
)
PARTITION BY DATE(_ingest_ts)
CLUSTER BY user_id, event_name;
