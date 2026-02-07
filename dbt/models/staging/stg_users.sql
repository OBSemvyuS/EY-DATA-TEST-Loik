{{
  config(materialized='view')
}}
select
  id as user_id,
  email,
  username,
  name.firstname as first_name,
  name.lastname as last_name,
  address.city as city,
  address.street as street,
  address.zipcode as zipcode,
  address.geolocation.lat as lat,
  address.geolocation.long as long,
  phone
from {{ source('fakestore', 'raw_users_external') }}
