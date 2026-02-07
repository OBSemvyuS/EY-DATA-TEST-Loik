# dbt (BigQuery)

Project dbt for the EY Data test. Transforms raw Fake Store data in BigQuery into staging views.

## Prerequisites

- Python 3 with `dbt-bigquery` installed (`pip install -r ../requirements.txt` or `pip install dbt-bigquery`)
- GCP Application Default Credentials: `gcloud auth application-default login`
- BigQuery dataset `fakestore` and raw tables: `raw_products_external`, `raw_users_external`, `raw_carts`

## Profile

Uses **ADC only** (no service account key). `profiles.yml` is in this folder. Run from the `dbt/` directory so dbt finds it, or copy to `~/.dbt/profiles.yml`.

## Commands (from `dbt/`)

```bash
cd dbt

# Use project profiles
export DBT_PROFILES_DIR=$PWD

dbt deps   # install packages (none required for now)
dbt run    # build staging views
dbt test   # run model and source tests
dbt docs generate && dbt docs serve  # optional: docs UI
```

## Structure

- **sources** (`models/sources.yml`): `raw_products_external`, `raw_users_external`, `raw_carts`
- **staging** (`models/staging/`): `stg_products`, `stg_users`, `stg_carts` (views in dataset `fakestore`)

Output views: `ey-data-test-486710.fakestore.stg_products`, `stg_users`, `stg_carts`.
