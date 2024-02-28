-- parent levels child levels intermediate model
-- materialized as a table for speed

with 
parent_levels_child_levels as (
    select 
        parent_level_id,
        child_level_id,
        position,
        kind 
    from {{ ref('base_dashboard__parent_levels_child_levels') }}
)

select * 
from parent_levels_child_levels