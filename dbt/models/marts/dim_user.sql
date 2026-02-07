{{
  config(
    materialized='table',
    description='Dimension table: users (one row per user_id).'
  )
}}
select
  user_id,
  email,
  username,
  first_name,
  last_name,
  city,
  street,
  zipcode,
  lat,
  long,
  phone
from {{ ref('stg_users') }}
qualify row_number() over (partition by user_id order by email) = 1
