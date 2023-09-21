with 
source as (
    select * 
    from "dashboard"."dashboard_production"."school_districts"
),

renamed as (
    select
        id      as school_district_id,
        name    as school_district_name,
        city    as school_district_city,
        state   as school_district_state,
        zip     as school_district_zip,
        last_known_school_year_open,
        created_at,
        updated_at
    from source
)

select * 
from renamed