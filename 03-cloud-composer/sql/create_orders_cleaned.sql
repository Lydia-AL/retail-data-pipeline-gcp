CREATE OR REPLACE TABLE
  `your-project-id.retail.orders_cleaned`
PARTITION BY order_date
CLUSTER BY country, category, status
AS
SELECT
  SAFE_CAST(order_id AS INT64) AS order_id,
  SAFE_CAST(order_date AS DATE) AS order_date,
  UPPER(TRIM(country)) AS country,
  LOWER(TRIM(category)) AS category,
  COALESCE(
    SAFE_CAST(quantity AS INT64),
    0
  ) AS quantity,
  COALESCE(
    SAFE_CAST(price AS FLOAT64),
    0.0
  ) AS price,
  LOWER(TRIM(status)) AS status,
  LOWER(TRIM(email)) AS email
FROM
  `your-project-id.retail.orders_raw`
WHERE
  order_id IS NOT NULL
  AND order_date IS NOT NULL;
