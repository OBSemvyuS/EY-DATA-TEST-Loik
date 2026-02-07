{{
  config(materialized='view')
}}
select
  id as product_id,
  title,
  price,
  description,
  category,
  image,
  rating.rate as rating_rate,
  rating.count as rating_count
from {{ source('fakestore', 'raw_products_external') }}
