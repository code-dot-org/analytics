with 
source as (
      select * from "dashboard"."pegasus_pii"."form_geos"
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

select * from renamed