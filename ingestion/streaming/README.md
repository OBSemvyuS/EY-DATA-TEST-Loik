# Near real-time ingestion (Carts)

## Prerequisites

- GCP project with Pub/Sub topic `carts-events` and subscription `carts-to-bq`
- BigQuery dataset `fakestore` and table `raw_carts`

Create the table (once) with the provided schema:

```bash
cd ingestion/streaming
bq mk --table ey-data-test-486710:fakestore.raw_carts raw_carts_schema.json
```

Or create the table in the BigQuery console with columns: `event_id` (STRING), `extracted_at` (TIMESTAMP), `published_at` (TIMESTAMP), `cart_id` (INTEGER), `user_id` (INTEGER), `cart_date` (DATE), `products_json` (STRING).

## Safe limits (free tier)

- **Publisher**: `--interval 60 --max-polls 5` (or 10)
- **Subscriber**: `--max-messages 100` (default)

## Run

1. **Terminal 1 – Subscriber** (start first, so it is ready to consume):

   ```bash
   source venv/bin/activate  # from project root
   python ingestion/streaming/subscribe_carts_to_bq.py --max-messages 100
   ```

2. **Terminal 2 – Publisher** (poll API and publish events):

   ```bash
   source venv/bin/activate
   python ingestion/streaming/publish_carts.py --interval 60 --max-polls 5
   ```

Stop with Ctrl+C when done. The subscriber will stop when it has processed `--max-messages` or you interrupt it.
