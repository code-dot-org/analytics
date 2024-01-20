with 
source as (
    select * 
    from "dashboard"."dashboard_production"."parent_levels_child_levels"
),

renamed as (
    select
        id as parent_levels_child_level_id,
        parent_level_id,
        child_level_id,
        position,
        kind
    from source
)

select *
from renamed