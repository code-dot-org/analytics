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
        created_at,
        updated_at,
        lockable            as is_lesson_lockable,
        relative_position,
        properties,
        lesson_group_id,
        key,
        has_lesson_plan
    from source
)

select * 
from renamed