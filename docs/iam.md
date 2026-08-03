# IAM and service accounts

This project uses several Google Cloud managed identities and runtime service accounts.

## Pub/Sub BigQuery subscription

A native BigQuery subscription uses the Pub/Sub service agent:

```text
service-<PROJECT_NUMBER>@gcp-sa-pubsub.iam.gserviceaccount.com
```

The service agent needs permission to write to the destination BigQuery table.

During the training project, the role used was:

```text
BigQuery Data Editor
```

Dataset-level access should be preferred over project-level access whenever possible.

## Cloud Storage-triggered function

The runtime service account needs permission to:

- read objects from the source Cloud Storage bucket;
- insert rows into the destination BigQuery table.

Typical permissions may be provided through roles such as:

- Storage Object Viewer;
- BigQuery Data Editor.

## Pub/Sub-triggered function

The runtime service account needs permission to:

- receive the triggered event;
- insert transformed rows into BigQuery.

Eventarc and Pub/Sub managed service agents may also require permissions depending on the deployment configuration.

## Cloud Composer

The Cloud Composer service account needs permission to:

- execute Composer worker operations;
- read files from Cloud Storage;
- create BigQuery jobs;
- read and write BigQuery tables.

## Security principles

This repository does not contain:

- service-account keys;
- passwords;
- access tokens;
- personal credentials;
- real customer data.

All sample data is synthetic.

IAM permissions should follow the principle of least privilege.
