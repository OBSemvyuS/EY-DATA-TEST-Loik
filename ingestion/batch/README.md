# Batch ingestion (Products & Users)

## Output format

The script writes **NDJSON** (one JSON object per line) to GCS so that BigQuery external tables can read one row per product/user.

Paths:

- `gs://BUCKET/raw/products/date=YYYY-MM-DD/products.json`
- `gs://BUCKET/raw/users/date=YYYY-MM-DD/users.json`

## Run

```bash
# From project root, with venv activated
python ingestion/batch/ingest_batch.py
# Or with a specific date:
python ingestion/batch/ingest_batch.py --date 2025-02-07
```

## BigQuery external tables (Option A)

After the first batch run, create the external tables so dbt (or SQL) can read the raw data without loading it into native tables:

```bash
cd ingestion/batch/sql
bq query --use_legacy_sql=false < create_external_tables.sql
```

Or run the contents of `create_external_tables.sql` in the BigQuery console.

Tables created:

- `fakestore.raw_products_external` → reads all `raw/products/*/*.json`
- `fakestore.raw_users_external` → reads all `raw/users/*/*.json`

If you already had batch data in the old JSON array format, re-run the batch script once so that new files are NDJSON; the external tables will then pick them up.
