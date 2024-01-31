with 
teachers as (
    select 
    {{ dbt_utils.star(
        from=ref('stg_dashboard_pii__users'),
        except=["user_id",
            "is_urg",
            "student_id",
            "birthday",
            "school_info_id"]) }}
    from {{ ref('stg_dashboard_pii__users') }}
    where teacher_id is not null 
),

user_geos as (
    select 
        user_id, 
        is_international
    from {{ ref('stg_dashboard__user_geos') }}
    where user_id in (select teacher_id from teachers)
),

user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
), 

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

-- get teacher NCES school_id association
teacher_schools as (
    select 
        teachers.teacher_id,
        si.school_id,
        rank () over (partition by teachers.teacher_id order by si.school_id, usi.ended_at desc) rnk
    from teachers
    left join user_school_infos as usi    
        on usi.user_id = teachers.teacher_id
    left join school_infos as si 
        on si.school_info_id = usi.school_info_id
    where si.school_id is not null
),

teacher_latest_school as (
    select 
        teacher_id,
        school_id
    from teacher_schools
    where rnk = 1
)

select 
    teachers.*, 
    school_years.school_year as created_at_school_year,
    tls.school_id,
    user_geos.is_international
from teachers 
left join user_geos 
    on teachers.teacher_id = user_geos.user_id
left join teacher_latest_school tls 
    on tls.teacher_id = user_geos.user_id
left join school_years 
    on teachers.created_at between school_years.started_at and school_years.ended_at
