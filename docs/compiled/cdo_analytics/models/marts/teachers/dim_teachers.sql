with 
teachers as (
    select "user_id",
  "studio_person_id",
  "sign_in_count",
  "current_sign_in_at",
  "last_sign_in_at",
  "created_at",
  "updated_at",
  "provider",
  "gender",
  "locale",
  "user_type",
  "school_info_id",
  "total_lines",
  "is_active",
  "deleted_at",
  "purged_at",
  "invited_by_id",
  "invited_by_type",
  "terms_of_service_version",
  "is_urm",
  "races"
    from "dev"."dbt_allison"."stg_dashboard__users"
    where user_type = 'teacher'
        and purged_at is null 
        and created_at >= '2023-01-01'
),

user_geos as (
    select 
        user_id,
        is_international,
        lower(country) as country
    from "dev"."dbt_allison"."stg_dashboard__user_geos"
    where user_id in (select user_id from teachers)
),

 

combined as (
    select 
        teachers.user_id as teacher_user_id,
        teachers.gender,
        teachers.is_urm,
        teachers.races,
        teachers.is_active,
        teachers.school_info_id,
        ug.is_international,
        ug.country
    from teachers 
    left join user_geos as ug 
        on teachers.user_id = ug.user_id
)

select * 
from combined