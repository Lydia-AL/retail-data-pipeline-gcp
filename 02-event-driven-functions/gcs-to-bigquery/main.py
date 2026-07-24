import io
import os

import functions_framework
import pandas as pd
from google.cloud import bigquery, storage


# Google Cloud clients are initialized once when the function instance starts.
storage_client = storage.Client()
bigquery_client = bigquery.Client()


# Runtime configuration
PROJECT_ID = os.getenv("PROJECT_ID") or bigquery_client.project
BQ_DATASET = os.getenv("BQ_DATASET", "retail")
BQ_TABLE = os.getenv("BQ_TABLE", "orders_raw")


@functions_framework.cloud_event
def hello_gcs(cloud_event):
    """
    Load a CSV file uploaded to Cloud Storage into BigQuery.

    The function is triggered when a new object is created
    in the configured Cloud Storage bucket.
    """

    event_data = cloud_event.data

    bucket_name = event_data["bucket"]
    object_name = event_data["name"]

    print(
        {
            "event_id": cloud_event["id"],
            "event_type": cloud_event["type"],
            "bucket": bucket_name,
            "object": object_name,
        }
    )

    # Ignore files that are not CSV files.
    if not object_name.lower().endswith(".csv"):
        print(f"Skipping non-CSV file: {object_name}")
        return

    # Download the CSV file from Cloud Storage.
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    file_content = blob.download_as_bytes()

    # Read the CSV content into a Pandas DataFrame.
    dataframe = pd.read_csv(io.BytesIO(file_content))

    print(
        f"CSV loaded successfully: "
        f"{len(dataframe)} rows and "
        f"{len(dataframe.columns)} columns"
    )

    # Pandas represents missing values with NaN.
    # BigQuery expects Python None values for nullable fields.
    rows = dataframe.where(
        pd.notna(dataframe),
        None,
    ).to_dict(orient="records")

    table_fqn = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # Insert the converted rows into BigQuery.
    errors = bigquery_client.insert_rows_json(
        table_fqn,
        rows,
    )

    if errors:
        raise RuntimeError(
            f"BigQuery insertion failed: {errors}"
        )

    print(
        f"Successfully inserted "
        f"{len(rows)} rows into {table_fqn}"
    )
