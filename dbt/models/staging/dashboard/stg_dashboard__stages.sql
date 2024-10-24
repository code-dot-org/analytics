with 
stages as (
    select *     
    from {{ ref('base_dashboard__stages') }}
),

renamed as (
    select 
        stage_id,
        lower(stage_name)                   as stage_name,
        
        case 
            when is_lockable = 1 
                then absolute_position 
            else relative_position 
        end as stage_number,
        
        script_id,
        lesson_group_id,
        
        absolute_position,
        relative_position,
        key,

        has_lesson_plan,
        is_lockable,
        is_unplugged,
        
        created_at,
        updated_at
    from stages)

select * 
from renamed   