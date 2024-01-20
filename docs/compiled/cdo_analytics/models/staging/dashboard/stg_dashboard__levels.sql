with 
 __dbt__cte__base_dashboard__levels as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production"."levels"
),

renamed as (
    select
        id                      as level_id,
        game_id,
        name,
        created_at,
        updated_at,
        level_num,
        ideal_level_source_id,
        user_id,
        properties,
        type,
        md5,
        published               as is_published,
        notes,
        audit_log
    from source
)

select * 
from renamed
), levels as (
    select * 
    from __dbt__cte__base_dashboard__levels
)

select * 
from levels