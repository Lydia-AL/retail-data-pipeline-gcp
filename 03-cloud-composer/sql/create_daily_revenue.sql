CREATE OR REPLACE TABLE
  `your-project-id.retail.daily_revenue`
PARTITION BY order_date
AS
SELECT
  order_date,
  country,
  category,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(quantity) AS units_sold,
  ROUND(
    SUM(quantity * price),
    2
  ) AS revenue
FROM
  `your-project-id.retail.orders_cleaned`
WHERE
  status NOT IN (
    'cancelled',
    'canceled',
    'refunded'
  )
GROUP BY
  order_date,
  country,
  category;
