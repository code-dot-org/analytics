with 
source as (
      select * from {{ source('dashboard', 'user_geos') }}
),

renamed as (
    select
        id as user_geo_id,
        user_id,
        created_at,
        updated_at,
        indexed_at,
        city,
        state,
        lower(country) as country,
        postal_code
    from source
)

select * 
from renamed