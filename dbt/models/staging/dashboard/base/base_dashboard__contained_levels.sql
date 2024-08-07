with 
source as (
    select * 
    from {{ source('dashboard', 'contained_levels') }}
),

renamed as (
    select
        id as contained_levels_id,
        level_group_level_id,
        contained_level_id,
        contained_level_type,
        contained_level_page,
        contained_level_position,
        contained_level_text,
        created_at,
        updated_at
    from source
)

select * 
from renamed