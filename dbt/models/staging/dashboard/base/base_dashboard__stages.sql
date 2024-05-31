with 
source as (
    select * 
    from {{ source('dashboard', 'stages') }}
),

renamed as (
    select
        id                  as stage_id,
        name                as stage_name,
        absolute_position,
        script_id,
        lockable            as is_lockable,
        relative_position,
        json_extract_path_text(
            properties, 
            'unplugged', 
            true)           as lesson_unplugged,

        {# properties, #}
        lesson_group_id,
        key,
        has_lesson_plan,
        
        created_at,
        updated_at
    from source
)

select * 
from renamed