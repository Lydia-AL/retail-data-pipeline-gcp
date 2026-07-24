# Cloud Storage to BigQuery

## Overview

This pipeline demonstrates an event-driven ingestion workflow on Google Cloud.

When a CSV file is uploaded to Cloud Storage, a Cloud Run function is triggered automatically. The function downloads the file, parses it with Pandas and inserts its rows into BigQuery.

## Architecture

```text
CSV file
    ↓
Cloud Storage bucket
    ↓
Object creation event
    ↓
Cloud Run function
    ↓
Pandas DataFrame
    ↓
BigQuery orders_raw
```

## Objective

The objective is to automate the ingestion of retail order files without manually launching a script each time a new file arrives.

This architecture can be used when a provider or business application regularly deposits files in a Cloud Storage bucket.

## Technologies

- Cloud Storage
- Cloud Run functions
- Eventarc
- BigQuery
- Python
- Pandas

## Processing steps

The function performs the following steps:

1. receives the Cloud Storage event;
2. retrieves the bucket and object names;
3. ignores files that are not CSV files;
4. downloads the file into memory;
5. reads the CSV with Pandas;
6. converts missing values into Python `None` values;
7. converts the DataFrame into JSON-compatible records;
8. inserts the rows into BigQuery.

## Entry point

The Cloud Run function entry point is:

```text
hello_gcs
```

## Environment variables

The function uses the following environment variables:

| Variable | Description | Default value |
|---|---|---|
| `PROJECT_ID` | Google Cloud project containing BigQuery | Runtime project |
| `BQ_DATASET` | Destination BigQuery dataset | `retail` |
| `BQ_TABLE` | Destination BigQuery table | `orders_raw` |

Environment variables make the function reusable across different Google Cloud environments without modifying the source code.

## Destination table

The default destination is:

```text
PROJECT_ID.retail.orders_raw
```

The table contains:

| Column | BigQuery type |
|---|---|
| `order_id` | `INT64` |
| `order_date` | `DATE` |
| `country` | `STRING` |
| `category` | `STRING` |
| `quantity` | `INT64` |
| `price` | `FLOAT64` |
| `status` | `STRING` |
| `email` | `STRING` |

## Repository files

```text
gcs-to-bigquery/
├── main.py
├── requirements.txt
├── schema.json
├── sample_data/
│   └── orders_raw.csv
└── sql/
    └── create_orders_raw.sql
```

## Sample data

The file `sample_data/orders_raw.csv` contains synthetic retail data.

It includes several intentional quality issues, such as:

- inconsistent uppercase and lowercase values;
- leading and trailing spaces;
- missing quantities;
- missing prices.

These imperfections are retained in the raw ingestion layer and will be handled later by the transformation workflow.

## Data quality considerations

The current function focuses on file ingestion.

A production implementation could also include:

- schema validation;
- required-column validation;
- invalid-row rejection;
- duplicate detection;
- file-level idempotency;
- a quarantine bucket;
- BigQuery load jobs for large files;
- structured logging;
- monitoring and alerting.

## Scalability consideration

The function currently uses:

```python
insert_rows_json()
```

This approach is suitable for demonstrating row insertion with small sample files.

For larger production files, a BigQuery load job would generally be more appropriate because it is optimized for batch ingestion.

## Security

The runtime service account needs permission to:

- read objects from the source Cloud Storage bucket;
- insert rows into the destination BigQuery table.

No credentials or service-account keys are stored in this repository.

## Skills demonstrated

- event-driven architecture;
- Cloud Storage event handling;
- Cloud Run function development;
- Python dependency management;
- CSV processing with Pandas;
- BigQuery row insertion;
- environment-variable configuration;
- cloud logging and troubleshooting.
