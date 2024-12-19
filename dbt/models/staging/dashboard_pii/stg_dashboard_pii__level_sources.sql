with 
level_sources as (
    select *
    from {{ ref('base_dashboard_pii__level_sources') }}
),

final as (
    select 
        level_source_id,
        level_id,
        data,
        created_at,
        updated_at
    from level_sources )

select *
from final 