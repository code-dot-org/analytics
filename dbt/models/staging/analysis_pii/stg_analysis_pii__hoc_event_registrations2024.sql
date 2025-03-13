with 
hoc_event_registrations as (
    select *
    from {{ ref('base_analysis_pii__hoc_event_registrations2024') }}
)

, renamed as (
    select 
        
        email
        , school as user_provided_school
        , job_title as user_provided_title
        , {{country_normalization('country')}} as country
        , last_submitted
        , language
    
    from hoc_event_registrations
    )

select * 
from renamed
