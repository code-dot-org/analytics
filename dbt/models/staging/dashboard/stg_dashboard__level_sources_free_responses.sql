with 
level_sources_free_responses as (
    select * 
    from {{ ref('base_dashboard__level_sources_free_responses') }}
),

renamed as (
    select 
        level_sources_free_response_id,
        level_id, 
        data,
        created_at,
        updated_at
    from level_sources_free_responses )

select * 
from renamed