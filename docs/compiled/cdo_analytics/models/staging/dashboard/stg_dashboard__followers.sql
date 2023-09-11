with 
 __dbt__cte__base_dashboard__followers as (
with 
source as (
      select * from "dashboard"."dashboard_production"."followers"
),

renamed as (
    select
        id as follower_id,
        student_user_id,
        section_id,
        created_at,
        updated_at
        deleted_at
    from source
)

select * from renamed
), followers as (
    select * 
    from __dbt__cte__base_dashboard__followers
)

select * from followers