with 
source as (
    select * 
    from {{ source('dashboard', 'stages') }}
),

renamed as (
    select
        id                  as stage_id,
        name                as stage_name,
        script_id,
        lesson_group_id,
        absolute_position,
        relative_position,
        lockable            as is_lockable,
        has_lesson_plan,
        key,
        
        json_extract_path_text(
            properties, 
            'unplugged', 
            true)           as is_unplugged,
        
        created_at,
        updated_at
    from source
)

select * 
from renamed