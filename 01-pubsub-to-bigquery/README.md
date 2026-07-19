# Pub/Sub to BigQuery

## Overview

This project demonstrates how to ingest streaming events from Google Cloud Pub/Sub directly into a partitioned BigQuery table without writing any custom consumer.

## Architecture

Publisher
    ↓
Pub/Sub Topic
    ↓
BigQuery Subscription
    ↓
BigQuery (orders_streaming)

## Technologies

- Pub/Sub
- BigQuery
- IAM

## What I implemented

- Created a partitioned BigQuery table.
- Configured a Pub/Sub topic.
- Granted the Pub/Sub service agent permission to write into BigQuery.
- Published sample retail events.
- Verified streaming ingestion.

## Files

- `sql/create_orders_streaming.sql`
- `messages/sample_events.json`
- `scripts/publish_messages.sh`

## Skills demonstrated

- Event streaming
- BigQuery partitioning
- IAM configuration
- JSON event publishing
