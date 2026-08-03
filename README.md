# Retail Data Pipelines on Google Cloud

This repository presents several Data Engineering pipelines built on Google Cloud around a retail use case.

The project explores streaming ingestion, event-driven processing, data transformation, PII masking and workflow orchestration using Pub/Sub, BigQuery, Cloud Storage, Cloud Run functions and Cloud Composer.

## Project overview

The repository contains four complementary pipelines:

1. native streaming ingestion from Pub/Sub to BigQuery;
2. event-driven CSV ingestion from Cloud Storage to BigQuery;
3. Pub/Sub processing with normalization and email masking;
4. daily ELT orchestration with Cloud Composer and Apache Airflow.

## Architecture overview

```text
Pipeline 1

Publisher
    ↓
Pub/Sub
    ↓
BigQuery subscription
    ↓
BigQuery orders_streaming


Pipeline 2

CSV file
    ↓
Cloud Storage
    ↓
Cloud Run function
    ↓
Pandas
    ↓
BigQuery orders_raw


Pipeline 3

Pub/Sub transaction
    ↓
Cloud Run function
    ↓
Validation and normalization
    ↓
Email masking
    ↓
BigQuery orders_streaming_safe


Pipeline 4

Cloud Storage CSV
    ↓
Cloud Composer / Airflow
    ↓
BigQuery orders_raw
    ↓
BigQuery orders_cleaned
    ↓
BigQuery daily_revenue
```

A visual architecture diagram will be added to this section.

## Technologies

- Google Cloud Pub/Sub
- BigQuery
- Cloud Storage
- Cloud Run functions
- Eventarc
- Cloud Composer
- Apache Airflow
- Python
- SQL
- Pandas
- IAM

## Repository structure

```text
retail-data-pipeline-gcp/
│
├── README.md
├── .env.example
├── .gitignore
│
├── 01-pubsub-to-bigquery/
│   ├── README.md
│   ├── messages/
│   │   └── sample_events.json
│   ├── scripts/
│   │   └── publish_messages.sh
│   └── sql/
│       └── create_orders_streaming.sql
│
├── 02-event-driven-functions/
│   ├── gcs-to-bigquery/
│   │   ├── README.md
│   │   ├── main.py
│   │   ├── requirements.txt
│   │   ├── schema.json
│   │   ├── sample_data/
│   │   │   └── orders_raw.csv
│   │   └── sql/
│   │       └── create_orders_raw.sql
│   │
│   └── pubsub-to-bigquery-safe/
│       ├── README.md
│       ├── main.py
│       ├── requirements.txt
│       ├── messages/
│       │   └── sample_transaction.json
│       └── sql/
│           └── create_orders_streaming_safe.sql
│
├── 03-cloud-composer/
│   ├── README.md
│   ├── dags/
│   │   ├── daily_pipeline_step1.py
│   │   └── daily_pipeline.py
│   └── sql/
│       ├── create_orders_cleaned.sql
│       └── create_daily_revenue.sql
│
└── docs/
    ├── iam.md
    └── troubleshooting.md
```

## Pipeline 1 — Native Pub/Sub to BigQuery streaming

```text
Publisher
    ↓
Pub/Sub topic
    ↓
BigQuery subscription
    ↓
Partitioned BigQuery table
```

This pipeline streams retail order events directly from Pub/Sub to BigQuery.

It demonstrates that a custom consumer is not always required when messages can be inserted without transformation.

The destination table is partitioned using the event timestamp.

More details:

```text
01-pubsub-to-bigquery/README.md
```

## Pipeline 2 — Cloud Storage to BigQuery

```text
CSV file
    ↓
Cloud Storage
    ↓
Cloud Run function
    ↓
Pandas DataFrame
    ↓
BigQuery orders_raw
```

Uploading a CSV file to Cloud Storage triggers a Cloud Run function.

The function:

- reads the Cloud Storage event;
- downloads the file into memory;
- parses the content with Pandas;
- converts missing values;
- inserts the rows into BigQuery.

This architecture represents a common event-driven ingestion pattern for files delivered by external providers.

More details:

```text
02-event-driven-functions/gcs-to-bigquery/README.md
```

## Pipeline 3 — Pub/Sub to BigQuery with PII masking

```text
Pub/Sub message
        ↓
Cloud Run function
        ↓
Validation
        ↓
Normalization
        ↓
Email masking
        ↓
BigQuery
```

This pipeline introduces a transformation layer between Pub/Sub and BigQuery.

The Cloud Run function:

- decodes the Base64 payload;
- parses the JSON message;
- validates required fields;
- normalizes country, category and status;
- converts numeric fields;
- masks the customer email address;
- adds an ingestion timestamp;
- inserts the transformed event into BigQuery.

Example:

```text
customer.secret@example.com
```

becomes:

```text
cu***@example.com
```

More details:

```text
02-event-driven-functions/pubsub-to-bigquery-safe/README.md
```

## Pipeline 4 — Cloud Composer orchestration

```text
orders_raw.csv
      ↓
load_to_staging
      ↓
orders_raw
      ↓
transform_cleaned
      ↓
orders_cleaned
      ↓
aggregate_daily
      ↓
daily_revenue
```

The Cloud Composer pipeline uses Apache Airflow to orchestrate three dependent tasks:

1. load the raw CSV from Cloud Storage;
2. clean and standardize the data in BigQuery;
3. calculate daily revenue.

The cleaned table is partitioned by date and clustered by business dimensions.

The daily revenue table contains:

- order count;
- units sold;
- revenue;
- country;
- product category.

More details:

```text
03-cloud-composer/README.md
```

## Configuration

The examples use placeholders instead of real Google Cloud identifiers.

Copy the environment template:

```bash
cp .env.example .env
```

Then configure:

```text
PROJECT_ID
REGION
RAW_BUCKET
BQ_DATASET
BQ_TABLE
```

Cloud Composer also requires the following Airflow variables:

```text
project_id
raw_bucket
bq_location
```

## Sample data

All sample data in this repository is synthetic.

The raw CSV intentionally contains several quality issues:

- inconsistent uppercase and lowercase values;
- leading and trailing spaces;
- missing quantities;
- missing prices.

These imperfections illustrate the difference between:

- a raw ingestion layer;
- a cleaned transformation layer;
- an aggregated business layer.

## Data model

### Raw table

```text
retail.orders_raw
```

Contains the source data without business transformations.

### Cleaned table

```text
retail.orders_cleaned
```

Contains normalized and validated order data.

### Aggregated table

```text
retail.daily_revenue
```

Contains daily business metrics by country and category.

### Safe streaming table

```text
ecom_dataset.orders_streaming_safe
```

Contains normalized streaming events with masked email addresses.

## Security and IAM

The project uses several Google Cloud identities:

- Pub/Sub service agent;
- Cloud Run function runtime service accounts;
- Cloud Composer service account.

Each identity should receive only the permissions required for its pipeline.

No credentials or service-account keys are stored in this repository.

More details:

```text
docs/iam.md
```

## Production considerations

These implementations focus on demonstrating the core GCP architectures.

A production-ready solution could add:

- explicit schema contracts;
- dead-letter topics;
- rejected-file quarantine;
- idempotency;
- duplicate detection;
- incremental loading;
- data-quality tests;
- structured logging;
- monitoring and alerting;
- automated unit and integration tests;
- continuous integration;
- separate development and production environments;
- secret management;
- policy tags and column-level security.

## Cost control

Managed cloud services can generate costs while resources remain active.

After testing, unused resources should be deleted or disabled, particularly:

- Cloud Composer environments;
- Cloud Run functions;
- Eventarc triggers;
- Pub/Sub topics and subscriptions;
- Cloud Storage buckets;
- BigQuery datasets and tables.

## What I learned

Through this project, I practiced:

- designing batch and streaming ingestion pipelines;
- connecting Pub/Sub directly to BigQuery;
- building event-driven Cloud Run functions;
- reading Cloud Storage events;
- decoding Pub/Sub messages;
- processing CSV files with Pandas;
- validating and normalizing event data;
- masking personally identifiable information;
- defining BigQuery schemas;
- using partitioning and clustering;
- creating Apache Airflow DAGs;
- managing task dependencies;
- orchestrating ELT workflows with Cloud Composer;
- configuring IAM roles and service accounts;
- troubleshooting distributed cloud pipelines.

## Project context

This project was developed as part of a Google Cloud Data Engineering learning journey.

The original training exercises were reorganized and documented as a single end-to-end portfolio project to demonstrate several complementary Data Engineering patterns.
