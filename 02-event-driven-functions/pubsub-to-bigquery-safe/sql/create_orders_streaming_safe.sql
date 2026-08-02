CREATE SCHEMA IF NOT EXISTS
  `your-project-id.ecom_dataset`
OPTIONS (
  location = "europe-west9"
);

CREATE TABLE IF NOT EXISTS
  `your-project-id.ecom_dataset.orders_streaming_safe`
(
  order_id INT64,
  event_time TIMESTAMP,
  country STRING,
  category STRING,
  quantity INT64,
  price NUMERIC,
  status STRING,
  email STRING,
  ingested_at TIMESTAMP
)
PARTITION BY DATE(event_time)
CLUSTER BY country, category, status;
