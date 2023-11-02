with 
stages as (
    select 
        stage_id,
        stage_name,
        case when is_lockable = 1 then absolute_position else relative_position end as stage_number,
        absolute_position,
        script_id,
        created_at,
        updated_at,
        is_lockable,
        relative_position,
        -- properties,
        lesson_group_id,
        key,
        has_lesson_plan        
    from {{ ref('base_dashboard__stages') }}
)

select * 
from stages