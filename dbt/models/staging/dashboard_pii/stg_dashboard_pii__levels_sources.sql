with 
level_sources as (
    select *, row_number () over (order by created_at) as row_num 
    from {{ ref('base_dashboard_pii__level_sources') }}
),

final as (
    select 
        level_sources_id,
        level_id,
        created_at,
        updated_at
    from level_sources
    where row_num = 1
)

select *
final 