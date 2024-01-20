with 
 __dbt__cte__base_dashboard__unit_groups as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production"."unit_groups"
),

renamed as (
    select
        id as unit_group_id,
        name,
        properties,
        created_at,
        updated_at,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience
    from source
)

select * 
from renamed
), unit_groups as (
    select * 
    from __dbt__cte__base_dashboard__unit_groups
)

select * 
from unit_groups