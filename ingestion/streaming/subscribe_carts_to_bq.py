"""
Subscriber: pull cart events from Pub/Sub, insert into BigQuery raw_carts (streaming).
Uses ADC. Safe limits: --max-messages 100 or --max-duration 600.
Requires table fakestore.raw_carts with schema:
  event_id STRING, extracted_at TIMESTAMP, published_at TIMESTAMP,
  cart_id INT64, user_id INT64, cart_date DATE, products_json STRING
"""

import argparse
import json
import os
import time
from typing import Optional

from google.cloud import bigquery, pubsub_v1

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "ey-data-test-486710")
SUBSCRIPTION_ID = "carts-to-bq"
DATASET_ID = "fakestore"
TABLE_ID = "raw_carts"


def message_to_row(msg_data: dict) -> dict:
    """Convert published payload to BigQuery row."""
    return {
        "event_id": msg_data.get("event_id"),
        "extracted_at": msg_data.get("extracted_at"),
        "published_at": msg_data.get("published_at"),
        "cart_id": msg_data.get("cart_id"),
        "user_id": msg_data.get("user_id"),
        "cart_date": msg_data.get("cart_date"),
        "products_json": json.dumps(msg_data.get("products") or []),
    }


def run(
    max_messages: Optional[int],
    max_duration_sec: Optional[int],
) -> None:
    """Pull messages from subscription, insert into BigQuery until limit."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    started = time.monotonic()
    total_processed = 0

    while True:
        if max_messages is not None and total_processed >= max_messages:
            print(f"Stopped: max_messages={max_messages} reached.")
            break
        if max_duration_sec is not None and (time.monotonic() - started) >= max_duration_sec:
            print(f"Stopped: max_duration={max_duration_sec}s reached.")
            break

        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 10},
            timeout=5.0,
        )
        if not response.received_messages:
            time.sleep(2)
            continue

        rows = []
        ack_ids = []
        for msg in response.received_messages:
            try:
                data = json.loads(msg.message.data.decode("utf-8"))
                rows.append(message_to_row(data))
                ack_ids.append(msg.ack_id)
            except Exception as e:
                print(f"Skip message: {e}")

        if rows:
            errors = bq_client.insert_rows_json(table_ref, rows)
            if errors:
                print(f"BigQuery insert errors: {errors}")
            else:
                subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
                total_processed += len(rows)
                print(f"Inserted {len(rows)} rows (total {total_processed})")

    print(f"Done. Total rows inserted into BigQuery: {total_processed}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull cart events from Pub/Sub, insert into BigQuery")
    parser.add_argument("--project", type=str, default=os.environ.get("GCP_PROJECT_ID", "ey-data-test-486710"))
    parser.add_argument("--max-messages", type=int, default=100, help="Stop after this many messages (default 100)")
    parser.add_argument("--max-duration", type=int, default=None, help="Stop after this many seconds (optional)")
    args = parser.parse_args()

    global PROJECT_ID
    PROJECT_ID = args.project

    run(args.max_messages, args.max_duration)


if __name__ == "__main__":
    main()
