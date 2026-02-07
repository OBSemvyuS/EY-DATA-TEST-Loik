{{
  config(
    materialized='table',
    description='Fact table: one row per product line in a cart (star schema).'
  )
}}
select
  c.event_id,
  c.cart_id,
  c.user_id,
  date(c.cart_date) as cart_date,
  cast(json_extract_scalar(item, '$.productId') as int64) as product_id,
  cast(json_extract_scalar(item, '$.quantity') as int64) as quantity
from {{ ref('stg_carts') }} as c
cross join unnest(json_extract_array(c.products_json)) as item
where json_extract_scalar(item, '$.productId') is not null
