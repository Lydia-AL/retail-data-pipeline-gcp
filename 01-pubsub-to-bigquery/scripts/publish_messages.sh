#!/usr/bin/env bash

set -euo pipefail

TOPIC_NAME="${TOPIC_NAME:-orders-topic}"

gcloud pubsub topics publish "$TOPIC_NAME" \
  --message='{"event_id":"evt-3001","event_type":"order_created","event_timestamp":"2025-08-22T08:10:00Z","order_id":"O-100047","customer_id":"C-222","item_id":"SKU-PS5","item_category":"gaming","amount":549.00,"currency":"EUR","store_id":"FR-PARIS-01","country_code":"FR","source":"web"}' \
  --attribute=event_type=order_created,source=web,schema_version=v1

gcloud pubsub topics publish "$TOPIC_NAME" \
  --message='{"event_id":"evt-3002","event_type":"payment_authorized","event_timestamp":"2025-08-22T08:10:06Z","order_id":"O-100047","customer_id":"C-222","item_id":"SKU-PS5","item_category":"gaming","amount":549.00,"currency":"EUR","store_id":"FR-PARIS-01","country_code":"FR","source":"payments"}' \
  --attribute=event_type=payment_authorized,source=payments,schema_version=v1
