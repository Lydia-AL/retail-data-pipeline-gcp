# Pub/Sub to BigQuery with PII masking

## Overview

This pipeline demonstrates how to process retail transactions from Pub/Sub before inserting them into BigQuery.

Unlike a native Pub/Sub-to-BigQuery subscription, this architecture introduces a Cloud Run function between Pub/Sub and BigQuery.

The function validates the incoming message, normalizes several fields and masks the customer email address before storage.

## Architecture

```text
Retail application
        ↓
Pub/Sub topic
        ↓
Cloud Run function
        ↓
Validation and normalization
        ↓
Email masking
        ↓
BigQuery orders_streaming_safe
```

## Objective

The objective is to demonstrate when a custom processing layer is useful between Pub/Sub and BigQuery.

A direct BigQuery subscription is appropriate when messages can be stored without transformation.

A Cloud Run function becomes useful when the data must be:

- validated;
- normalized;
- enriched;
- anonymized;
- rejected when invalid.

## Technologies

- Pub/Sub
- Cloud Run functions
- Eventarc
- BigQuery
- Python
- IAM

## Processing steps

The function performs the following steps:

1. receives the Pub/Sub event;
2. extracts the Base64-encoded payload;
3. decodes the message;
4. parses the JSON content;
5. validates required fields;
6. converts numeric values to the expected types;
7. normalizes country, category and status values;
8. masks the customer email address;
9. adds an ingestion timestamp;
10. inserts the transformed row into BigQuery.

## Entry point

```text
on_pubsub_message
```

## Environment variables

| Variable | Description | Default value |
|---|---|---|
| `PROJECT_ID` | Google Cloud project | Runtime project |
| `BQ_DATASET` | Destination dataset | `ecom_dataset` |
| `BQ_TABLE` | Destination table | `orders_streaming_safe` |

## Transformations

### Country

Input:

```text
 fr
```

Output:

```text
FR
```

### Category

Input:

```text
Electronics
```

Output:

```text
electronics
```

### Status

Input:

```text
PAID
```

Output:

```text
paid
```

### Email masking

Input:

```text
customer.secret@example.com
```

Output:

```text
cu***@example.com
```

The function keeps only the first two characters of the email local part.

## Destination table

```text
PROJECT_ID.ecom_dataset.orders_streaming_safe
```

The table contains:

| Column | BigQuery type |
|---|---|
| `order_id` | `INT64` |
| `event_time` | `TIMESTAMP` |
| `country` | `STRING` |
| `category` | `STRING` |
| `quantity` | `INT64` |
| `price` | `NUMERIC` |
| `status` | `STRING` |
| `email` | `STRING` |
| `ingested_at` | `TIMESTAMP` |

The table is partitioned by event date and clustered by:

- country;
- category;
- status.

## Test message

The file `messages/sample_transaction.json` contains a synthetic example.

It can be published with the Google Cloud CLI:

```bash
gcloud pubsub topics publish transactions-topic \
  --message='{
    "order_id": 42,
    "event_time": "2026-07-10T12:00:00Z",
    "country": " fr ",
    "category": "Electronics",
    "quantity": 2,
    "price": 199.99,
    "status": "PAID",
    "email": "customer.secret@example.com"
  }'
```

## Security

The function demonstrates a simple PII protection mechanism by masking customer email addresses before storage.

In production, additional protections could include:

- hashing;
- tokenization;
- encryption;
- access policies;
- column-level security;
- policy tags;
- data-retention rules.

## Production improvements

A production implementation could add:

- Pub/Sub dead-letter topics;
- retry policies;
- schema validation with Pydantic or JSON Schema;
- idempotency using event identifiers;
- duplicate detection;
- structured logging;
- monitoring and alerting;
- automated tests;
- invalid-message quarantine.

## Skills demonstrated

- Pub/Sub message processing;
- Base64 decoding;
- JSON parsing;
- data validation;
- field normalization;
- PII masking;
- BigQuery streaming insertion;
- partitioning and clustering;
- environment-variable configuration.
