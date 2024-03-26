with 
source as (
    select * 
    from {{ source('dashboard', 'script_levels') }}
),

renamed as (
    select
        id                          as script_level_id,
        script_id,
        chapter,
        stage_id,
        position,
        seed_key,
        activity_section_id,
        activity_section_position,
        
        assessment                  as is_assessment,
        named_level                 as is_named_level,
        bonus                       as is_bonus,
        
        properties,
        
        created_at,
        updated_at
    from source
)

select * 
from renamed