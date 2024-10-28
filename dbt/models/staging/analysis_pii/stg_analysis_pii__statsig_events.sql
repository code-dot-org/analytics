with 
statsig_events as (
    select *
    from {{ ref('base_analysis_pii__statsig_events') }} 
),

renamed as (
    select 
        user_id,
        stable_id,	
        custom_ids,	
        timestamp   as event_at,
        event_name,	
        event_value,	
        user_object,	
        statsig_metadata,	
        company_metadata
    from statsig_events )

select * 
from renamed
