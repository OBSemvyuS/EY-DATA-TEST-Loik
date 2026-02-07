-- Create external tables pointing to raw JSON (NDJSON) in GCS.
-- Run with: bq query --use_legacy_sql=false < create_external_tables.sql
-- Or in BigQuery console: run each statement.
-- Replace PROJECT_ID and BUCKET if different.

-- Products (Fake Store API)
CREATE OR REPLACE EXTERNAL TABLE `ey-data-test-486710.fakestore.raw_products_external` (
  id INT64,
  title STRING,
  price FLOAT64,
  description STRING,
  category STRING,
  image STRING,
  rating STRUCT<rate FLOAT64, count INT64>
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://ey-data-test-486710-raw/raw/products/*.json']
);

-- Users (Fake Store API)
CREATE OR REPLACE EXTERNAL TABLE `ey-data-test-486710.fakestore.raw_users_external` (
  id INT64,
  email STRING,
  username STRING,
  password STRING,
  name STRUCT<firstname STRING, lastname STRING>,
  address STRUCT<
    city STRING,
    street STRING,
    number STRING,
    zipcode STRING,
    geolocation STRUCT<lat STRING, long STRING>
  >,
  phone STRING,
  __v INT64
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://ey-data-test-486710-raw/raw/users/*.json']
);
