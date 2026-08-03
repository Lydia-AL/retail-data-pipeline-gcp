from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


PROJECT_ID = "{{ var.value.project_id }}"
RAW_BUCKET = "{{ var.value.raw_bucket }}"
DATASET = "retail"


with DAG(
    dag_id="daily_retail_pipeline",
    description=(
        "Load, clean and aggregate retail order data."
    ),
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "gcp", "elt"],
) as dag:

    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket=RAW_BUCKET,
        source_objects=["orders_raw.csv"],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET}.orders_raw"
        ),
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    transform_cleaned = BigQueryInsertJobOperator(
        task_id="transform_cleaned",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE
                      `{PROJECT_ID}.{DATASET}.orders_cleaned`
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
                      `{PROJECT_ID}.{DATASET}.orders_raw`
                    WHERE
                      order_id IS NOT NULL
                      AND order_date IS NOT NULL
                """,
                "useLegacySql": False,
            }
        },
        location="{{ var.value.bq_location }}",
    )

    aggregate_daily = BigQueryInsertJobOperator(
        task_id="aggregate_daily",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE
                      `{PROJECT_ID}.{DATASET}.daily_revenue`
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
                      `{PROJECT_ID}.{DATASET}.orders_cleaned`
                    WHERE
                      status NOT IN (
                        'cancelled',
                        'canceled',
                        'refunded'
                      )
                    GROUP BY
                      order_date,
                      country,
                      category
                """,
                "useLegacySql": False,
            }
        },
        location="{{ var.value.bq_location }}",
    )

    load_to_staging >> transform_cleaned >> aggregate_daily
