# Troubleshooting

## Pub/Sub messages do not appear in BigQuery

Check:

- the Pub/Sub topic;
- the BigQuery subscription;
- the destination table name;
- the JSON message structure;
- the Pub/Sub service-agent permissions;
- the subscription error details.

## Cloud Storage function does not trigger

Check:

- the source bucket;
- the uploaded object name;
- the configured Eventarc trigger;
- the function region;
- the runtime service-account permissions;
- the Cloud Run function logs.

## BigQuery insertion fails

Check:

- the dataset and table names;
- the expected column names;
- the expected BigQuery types;
- nullable and required fields;
- the runtime service-account permissions;
- the function logs.

## CSV parsing fails

Check:

- the header row;
- the delimiter;
- the text encoding;
- the date format;
- numeric columns;
- empty values.

## Pub/Sub message cannot be decoded

Check:

- that the event contains a `message.data` field;
- that the payload is valid Base64;
- that the decoded content is valid JSON;
- that the required fields are present.

## Composer DAG import errors

Check:

- Python syntax;
- installed Airflow provider packages;
- Airflow variables;
- the DAG file location;
- BigQuery and Cloud Storage permissions.

Required Airflow variables:

```text
project_id
raw_bucket
bq_location
```

## Region mismatch

Cloud resources must use compatible regions.

Check the locations of:

- the BigQuery datasets;
- the Cloud Storage buckets;
- the Cloud Composer environment;
- Cloud Run functions;
- Eventarc triggers.

## Cost control

Cloud Composer environments can generate costs while they remain active.

After testing, delete or disable unused:

- Cloud Composer environments;
- Cloud Run functions;
- Eventarc triggers;
- Pub/Sub topics and subscriptions;
- Cloud Storage buckets;
- BigQuery datasets and tables.
