import base64
import json
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
from google.cloud import bigquery


bigquery_client = bigquery.Client()

PROJECT_ID = os.getenv("PROJECT_ID") or bigquery_client.project
BQ_DATASET = os.getenv("BQ_DATASET", "ecom_dataset")
BQ_TABLE = os.getenv("BQ_TABLE", "orders_streaming_safe")

BQ_FQN = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


def mask_email(email: str | None) -> str | None:
    """
    Mask the local part of an email address before storage.

    Example:
        customer.secret@example.com
        becomes
        cu***@example.com
    """

    if not email or "@" not in email:
        return None

    local_part, domain = email.split("@", 1)
    visible_prefix = local_part[:2]

    return f"{visible_prefix}***@{domain}"


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Validate and normalize an incoming retail transaction.
    """

    required_fields = [
        "order_id",
        "country",
        "category",
        "quantity",
        "price",
        "status",
    ]

    missing_fields = [
        field
        for field in required_fields
        if record.get(field) in (None, "")
    ]

    if missing_fields:
        raise ValueError(
            f"Missing required fields: {missing_fields}"
        )

    now_utc = datetime.now(
        timezone.utc
    ).isoformat()

    return {
        "order_id": int(record["order_id"]),
        "event_time": record.get("event_time") or now_utc,
        "country": str(record["country"]).strip().upper(),
        "category": str(record["category"]).strip().lower(),
        "quantity": int(record["quantity"]),
        "price": float(record["price"]),
        "status": str(record["status"]).strip().lower(),
        "email": mask_email(record.get("email")),
        "ingested_at": now_utc,
    }


@functions_framework.cloud_event
def on_pubsub_message(cloud_event):
    """
    Decode a Pub/Sub message, normalize its content,
    mask the email address and insert the result into BigQuery.
    """

    message = cloud_event.data.get("message", {})
    encoded_data = message.get("data")

    if not encoded_data:
        raise ValueError(
            "The Pub/Sub message does not contain a data field."
        )

    try:
        decoded_payload = base64.b64decode(
            encoded_data
        ).decode("utf-8")

        record = json.loads(decoded_payload)

    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Unable to decode Pub/Sub message: {error}"
        ) from error

    normalized_row = normalize_record(record)

    errors = bigquery_client.insert_rows_json(
        BQ_FQN,
        [normalized_row],
    )

    if errors:
        raise RuntimeError(
            f"BigQuery insertion failed: {errors}"
        )

    print(
        {
            "event_id": cloud_event["id"],
            "destination_table": BQ_FQN,
            "order_id": normalized_row["order_id"],
            "status": "inserted",
        }
    )
