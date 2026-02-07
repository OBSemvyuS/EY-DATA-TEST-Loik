"""
Publisher: poll Fake Store API Carts, publish each cart as an event to Pub/Sub.
Uses ADC. Safe limits: --interval 60 --max-polls 5 (or 10).
"""

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "ey-data-test-486710")
TOPIC_ID = "carts-events"
API_CARTS = "https://fakestoreapi.com/carts"


def fetch_carts() -> list:
    """Fetch carts from Fake Store API."""
    resp = requests.get(API_CARTS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def publish_cart_events(publisher: pubsub_v1.PublisherClient, topic_path: str, carts: list) -> int:
    """Publish each cart as a message with event metadata. Returns count published."""
    published = 0
    extracted_at = datetime.now(timezone.utc).isoformat()
    for cart in carts:
        published_at = datetime.now(timezone.utc).isoformat()
        event_id = str(uuid.uuid4())
        payload = {
            "event_id": event_id,
            "extracted_at": extracted_at,
            "published_at": published_at,
            "cart_id": cart.get("id"),
            "user_id": cart.get("userId"),
            "cart_date": cart.get("date"),
            "products": cart.get("products", []),
        }
        data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result(timeout=10)
        published += 1
    return published


def run(interval_sec: int, max_polls: Optional[int], max_duration_sec: Optional[int]) -> None:
    """Poll API at interval, publish to Pub/Sub, until max_polls or max_duration."""
    # ADC: no credentials passed
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    started = time.monotonic()
    poll_count = 0
    total_published = 0
    print(f"Publisher started. Polling every {interval_sec}s, max_polls={max_polls}. Ctrl+C to stop.", flush=True)

    try:
        while True:
            if max_polls is not None and poll_count >= max_polls:
                print(f"Stopped: max_polls={max_polls} reached.")
                break
            if max_duration_sec is not None and (time.monotonic() - started) >= max_duration_sec:
                print(f"Stopped: max_duration={max_duration_sec}s reached.")
                break

            try:
                carts = fetch_carts()
                n = publish_cart_events(publisher, topic_path, carts)
                total_published += n
                poll_count += 1
                print(f"Poll #{poll_count}: published {n} cart events (total {total_published})")
            except Exception as e:
                print(f"Error: {e}")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C).")

    print(f"Done. Total polls={poll_count}, total events published={total_published}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll Carts API and publish events to Pub/Sub")
    parser.add_argument("--project", type=str, default=os.environ.get("GCP_PROJECT_ID", "ey-data-test-486710"))
    parser.add_argument("--interval", type=int, default=60, help="Seconds between polls (default 60)")
    parser.add_argument("--max-polls", type=int, default=5, help="Stop after this many polls (default 5)")
    parser.add_argument("--max-duration", type=int, default=None, help="Stop after this many seconds (optional)")
    args = parser.parse_args()

    global PROJECT_ID
    PROJECT_ID = args.project

    run(args.interval, args.max_polls, args.max_duration)


if __name__ == "__main__":
    main()
