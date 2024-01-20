with 
 __dbt__cte__base_dashboard__levels_script_levels as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production"."levels_script_levels"
),

renamed as (
    select
        level_id,
        script_level_id
    from source
)

select * 
from renamed
), level_script_levels as (
    select * 
    from __dbt__cte__base_dashboard__levels_script_levels
)

select * 
from level_script_levels