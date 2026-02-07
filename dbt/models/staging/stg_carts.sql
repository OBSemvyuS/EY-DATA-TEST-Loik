{{
  config(materialized='view')
}}
select
  event_id,
  extracted_at,
  published_at,
  cart_id,
  user_id,
  cart_date,
  products_json
from {{ source('fakestore', 'raw_carts') }}
