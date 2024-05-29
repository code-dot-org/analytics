with 
projects as (
    select *
    from {{ ref('base_dashboard_pii__projects') }}
),

final as (
    select 
        project_id,
        project_type,
        storage_id,
        value,
        state,
        abuse_score,
        remix_parent_id,
        
        is_standalone,
        
        is_skip_content_moderation
        created_at,
        published_at,
        updated_at
    from projects )


select * 
from final 