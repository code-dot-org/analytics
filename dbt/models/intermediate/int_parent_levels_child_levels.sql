-- parent levels child levels intermediate model
-- materialized as a table for speed

with 
parent_levels_child_levels as (
    select 
        *
    from {{ ref('stg_dashboard__parent_levels_child_levels') }}
)

select * 
from parent_levels_child_levels