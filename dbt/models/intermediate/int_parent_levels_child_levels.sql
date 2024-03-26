-- parent levels child levels intermediate model
-- materialized as a table for speed

with 
parent_levels_child_levels as (
    select 
        parent_level_id,
        child_level_id,
        case 
            when parent_level_id is not null 
            then 1 else 0 
        end     as is_parent_level,

        kind    as parent_level_kind,
        position
    from {{ ref('base_dashboard__parent_levels_child_levels') }}
)

select * 
from parent_levels_child_levels