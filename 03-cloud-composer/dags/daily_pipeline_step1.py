from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


with DAG(
    dag_id="daily_pipeline_step1",
    description=(
        "Load the raw retail orders CSV "
        "from Cloud Storage into BigQuery."
    ),
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retail", "gcp", "ingestion"],
) as dag:

    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket="{{ var.value.raw_bucket }}",
        source_objects=["orders_raw.csv"],
        destination_project_dataset_table=(
            "{{ var.value.project_id }}."
            "retail.orders_raw"
        ),
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
