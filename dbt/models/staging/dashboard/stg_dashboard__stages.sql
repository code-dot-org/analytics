with 
stages as (
    select *     
    from {{ ref('base_dashboard__stages') }}
),

renamed as (
    select 
        stage_id,
        stage_name,
        
        case 
            when is_lesson_lockable = 1 
            then absolute_position 
            else relative_position 
        end as stage_number,

        absolute_position,
        relative_position,
        script_id,
        key,
        is_lesson_lockable,
        lesson_group_id,
        has_lesson_plan,

           -- json extraction fields 
        json_extract_path_text(
            properties, 
            'lesson_unplugged')    as is_lesson_unplugged,

        created_at,
        updated_at
    from stages)

select * 
from renamed   