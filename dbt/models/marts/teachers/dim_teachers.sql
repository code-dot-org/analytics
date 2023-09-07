with 
teachers as (
    select {{ dbt_utils.star(from=ref           ('stg_dashboard__users'),
        except=[
            "admin",
            "birthday",
            "primary_contact_info_id"]) }}
    from {{ ref('stg_dashboard__users')}}
    where user_type = 'teacher'
        and purged_at is null 
        and created_at >= '2023-01-01'
),

user_geos as (
    select 
        user_id,
        is_international,
        lower(country) as country
    from {{ ref('stg_dashboard__user_geos')}}
    where user_id in (select user_id from teachers)
),

 {# 
    school_info as (
        select *
        from {{ ref('dim_schools) }}
    ),
  #}

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