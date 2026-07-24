CREATE SCHEMA IF NOT EXISTS
  `your-project-id.retail`
OPTIONS (
  location = "europe-west9"
);

CREATE TABLE IF NOT EXISTS
  `your-project-id.retail.orders_raw`
(
  order_id INT64,
  order_date DATE,
  country STRING,
  category STRING,
  quantity INT64,
  price FLOAT64,
  status STRING,
  email STRING
);
