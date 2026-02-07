"""
Batch ingestion: fetch Products and Users from Fake Store API,
store raw JSON in GCS. Idempotent (date-based paths).
Uses Application Default Credentials (ADC) for GCP auth.
"""

import argparse
import json
import os
from datetime import date

import requests
from google.cloud import storage

# Config: bucket name from env or default
GCS_BUCKET = os.environ.get("GCS_BUCKET", "ey-data-test-486710-raw")
API_BASE = "https://fakestoreapi.com"


def fetch_json(url: str) -> list:
    """Fetch JSON from a REST API. Returns the response body as Python data."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def upload_to_gcs(client: storage.Client, bucket_name: str, path: str, data: str) -> None:
    """Upload a string (JSON) to GCS at the given path."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(data, content_type="application/json")


def run(ingestion_date: date) -> None:
    """Fetch Products and Users, upload raw JSON to GCS."""
    date_str = ingestion_date.isoformat()  # YYYY-MM-DD

    # 1. Fetch from APIs
    products = fetch_json(f"{API_BASE}/products")
    users = fetch_json(f"{API_BASE}/users")

    # 2. GCS client (ADC: no credentials passed)
    client = storage.Client()

    # 3. Upload raw JSON (idempotent: same path per date)
    upload_to_gcs(
        client,
        GCS_BUCKET,
        f"raw/products/date={date_str}/products.json",
        json.dumps(products, indent=2),
    )
    upload_to_gcs(
        client,
        GCS_BUCKET,
        f"raw/users/date={date_str}/users.json",
        json.dumps(users, indent=2),
    )

    print(f"Uploaded products and users to gs://{GCS_BUCKET}/raw/ for date={date_str}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch ingest Products and Users to GCS")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Ingestion date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.environ.get("GCS_BUCKET", "ey-data-test-486710-raw"),
        help="GCS bucket name (default: env GCS_BUCKET or ey-data-test-486710-raw)",
    )
    args = parser.parse_args()

    if args.date:
        ingestion_date = date.fromisoformat(args.date)
    else:
        ingestion_date = date.today()

    global GCS_BUCKET
    GCS_BUCKET = args.bucket

    run(ingestion_date)


if __name__ == "__main__":
    main()
