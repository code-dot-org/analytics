with 
source as (
    select * 
    from "dashboard"."dashboard_production"."ap_school_codes"
),

renamed as (
    select
        school_id,
        school_year,
        school_code,
        created_at,
        updated_at
    from source
)

select * 
from renamed