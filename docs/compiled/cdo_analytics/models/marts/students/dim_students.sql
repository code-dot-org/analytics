with 
students as (
    select 
    "student_id",
  "user_type",
  "gender",
  "is_urg",
  "locale",
  "sign_in_count",
  "total_lines",
  "current_sign_in_at",
  "last_sign_in_at",
  "created_at",
  "updated_at",
  "purged_at"
    from "dev"."dbt_jordan"."stg_dashboard__users"
    where student_id is not null 
),

user_geos as (
    select 
        user_id, 
        is_international
    from "dev"."dbt_jordan"."stg_dashboard__user_geos"
    where user_id in (select student_id from students)
),

school_years as (
    select * from "dev"."dbt_jordan"."int_school_years"
),

final as (
select 
    students.*, 
    user_geos.is_international,
    sy.school_year created_at_school_year
from students 
join user_geos 
    on students.student_id = user_geos.user_id
left join school_years sy on students.created_at between sy.started_at and sy.ended_at
)

select * 
from final