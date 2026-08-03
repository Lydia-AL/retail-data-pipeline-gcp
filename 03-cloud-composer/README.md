# Cloud Composer retail orchestration

## Overview

This pipeline uses Cloud Composer and Apache Airflow to orchestrate a daily ELT workflow for retail order data.

The workflow loads a raw CSV file from Cloud Storage, cleans the data in BigQuery and creates a daily revenue table.

## Architecture

```text
orders_raw.csv
      ↓
Cloud Storage
      ↓
load_to_staging
      ↓
BigQuery orders_raw
      ↓
transform_cleaned
      ↓
BigQuery orders_cleaned
      ↓
aggregate_daily
      ↓
BigQuery daily_revenue
```

## Objective

The objective is to demonstrate how Cloud Composer can coordinate several dependent data-processing tasks.

Instead of manually launching each operation, Airflow manages:

- task execution;
- scheduling;
- dependencies;
- retries;
- logs;
- workflow monitoring.

## Technologies

- Cloud Composer
- Apache Airflow
- Cloud Storage
- BigQuery
- Python
- SQL

## DAGs

### `daily_pipeline_step1.py`

This first DAG contains one task:

```text
load_to_staging
```

It loads `orders_raw.csv` from Cloud Storage into the BigQuery table:

```text
retail.orders_raw
```

### `daily_pipeline.py`

The complete DAG contains three tasks:

```text
load_to_staging
        ↓
transform_cleaned
        ↓
aggregate_daily
```

## Task 1 — Load raw data

Operator:

```text
GCSToBigQueryOperator
```

The task:

- reads the CSV from Cloud Storage;
- skips the header row;
- detects the schema;
- replaces the previous raw table content.

The write disposition is:

```text
WRITE_TRUNCATE
```

This makes the workflow reproducible because each run starts from the current source file.

## Task 2 — Clean the data

Operator:

```text
BigQueryInsertJobOperator
```

The transformation:

- converts country codes to uppercase;
- converts categories to lowercase;
- converts statuses to lowercase;
- removes leading and trailing spaces;
- converts invalid numeric values safely;
- replaces missing quantities with `0`;
- replaces missing prices with `0.0`;
- filters rows without an order identifier or date.

The cleaned table is partitioned by:

```text
order_date
```

It is clustered by:

- country;
- category;
- status.

## Task 3 — Aggregate daily revenue

The final task creates:

```text
retail.daily_revenue
```

The table contains:

- order date;
- country;
- product category;
- number of orders;
- units sold;
- revenue.

Revenue is calculated with:

```sql
SUM(quantity * price)
```

Cancelled and refunded orders are excluded from the calculation.

## Airflow variables

The DAG expects the following Airflow variables:

| Variable | Example |
|---|---|
| `project_id` | `your-project-id` |
| `raw_bucket` | `your-raw-bucket` |
| `bq_location` | `europe-west9` |

These variables allow the DAG to be reused without hardcoding environment-specific values.

## Schedule

The DAG runs:

```text
@daily
```

It also uses:

```python
catchup=False
```

This prevents Airflow from automatically executing all historical schedules between the start date and the current date.

The DAG uses:

```python
max_active_runs=1
```

This avoids running multiple instances of the same workflow simultaneously.

## IAM

The Cloud Composer environment service account needs permission to:

- read the source Cloud Storage bucket;
- create BigQuery jobs;
- read and write the target BigQuery tables;
- execute Composer worker operations.

Permissions should be granted according to the principle of least privilege.

## Production improvements

A production implementation could add:

- explicit schemas instead of autodetection;
- data-quality tests;
- row-count validation;
- failure notifications;
- retry configuration;
- task-level timeouts;
- incremental loading;
- separate development and production environments;
- SQL files loaded outside the Python DAG;
- continuous integration for DAG validation.

## Skills demonstrated

- Airflow DAG creation;
- task dependencies;
- Cloud Composer configuration;
- GCS-to-BigQuery ingestion;
- BigQuery ELT transformations;
- partitioning and clustering;
- daily data aggregation;
- workflow scheduling;
- runtime configuration with Airflow variables.
