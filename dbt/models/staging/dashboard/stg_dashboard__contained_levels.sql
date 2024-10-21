with 
contained_levels as (
    select * 
    from {{ ref("base_dashboard__contained_levels") }}
),

renamed as (
    select 
        contained_levels_id,
        level_group_level_id,
        contained_level_id,
        lower(contained_level_type)             as contained_level_type ,
        contained_level_page,
        contained_level_position,
        lower(contained_level_text)             as contained_level_text,
        created_at,
        updated_at
    from contained_levels )

select * 
from renamed 