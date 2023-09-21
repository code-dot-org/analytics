with 
 __dbt__cte__base_dashboard__user_levels as (
with 
source as (
      select * from "dashboard"."dashboard_production"."user_levels"
),

renamed as (
    select
        id                          as user_level_id,
        user_id,
        level_id,
        script_id,
        level_source_id,
        attempts,
        created_at,
        updated_at,
        best_result,
        time_spent,
        submitted                   as is_submitted,
        readonly_answers            as is_read_only_answers,
        unlocked_at,
        deleted_at,
        properties
    from source
)

select * from renamed where created_at > '2023-01-01'
), user_levels as (
    select * 
    from __dbt__cte__base_dashboard__user_levels
    where deleted_at is null 
)

select * from user_levels