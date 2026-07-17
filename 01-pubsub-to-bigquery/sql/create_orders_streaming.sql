CREATE SCHEMA IF NOT EXISTS `your-project-id.ecommerce`;

CREATE TABLE IF NOT EXISTS
  `your-project-id.ecommerce.orders_streaming`
(
  event_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  order_id STRING,
  customer_id STRING,
  item_id STRING,
  item_category STRING,
  amount NUMERIC,
  currency STRING,
  store_id STRING,
  country_code STRING,
  source STRING
)
PARTITION BY DATE(event_timestamp);
