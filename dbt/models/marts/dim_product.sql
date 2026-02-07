{{
  config(
    materialized='table',
    description='Dimension table: products (one row per product_id).'
  )
}}
select
  product_id,
  title,
  price,
  description,
  category,
  image,
  rating_rate,
  rating_count
from {{ ref('stg_products') }}
qualify row_number() over (partition by product_id order by title) = 1
