with 
source as (
    select * 
    from "dashboard"."pegasus_pii"."hoc_activity"
),

renamed as (
    select
        id                                      as hoc_activity_id,
        referer,
        company,
        tutorial,
        started_at,
        pixel_started_at,
        country_code,
        state_code,
        city,
        country,
        state
    from source
)

select * 
from renamed