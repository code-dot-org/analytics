with 
 __dbt__cte__base_pegasus_pii__form_geos as (
with 
source as (
    select * 
    from "dashboard"."pegasus_pii"."form_geos"
),

renamed as (
    select
        id as form_geo_id,
        form_id,
        created_at,
        updated_at,
        
        city,
        state,
        country,
        postal_code
        
    from source
)

select * 
from renamed
), form_geos as (
    select *
    from __dbt__cte__base_pegasus_pii__form_geos
)

select * 
from form_geos