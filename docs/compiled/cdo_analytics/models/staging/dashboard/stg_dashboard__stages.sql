with 
 __dbt__cte__base_dashboard__stages as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production"."stages"
),

renamed as (
    select
        id                  as stage_id,
        name                as stage_name,
        absolute_position,
        script_id,
        created_at,
        updated_at,
        lockable            as is_lockable,
        relative_position,
        -- properties,
        lesson_group_id,
        key,
        has_lesson_plan
    from source
)

select * 
from renamed
), stages as (
    select 
        stage_id,
        stage_name,
        case when is_lockable = 1 then absolute_position else relative_position end as stage_number,
        absolute_position,
        script_id,
        created_at,
        updated_at,
        is_lockable,
        relative_position,
        -- properties,
        lesson_group_id,
        key,
        has_lesson_plan        
    from __dbt__cte__base_dashboard__stages
)

select * 
from stages