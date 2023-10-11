with 
teachers as (
    select 
    {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=["user_id",
            "is_urg",
            "student_id"]) }}
    from {{ ref('stg_dashboard__users') }}
    where teacher_id is not null 
),

user_geos as (
    select 
        user_id, 
        is_international
    from {{ ref('stg_dashboard__user_geos') }}
    where user_id in (select teacher_id from teachers)
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
), 

-- get teacher NCES school_id association

teacher_schools as (
    select 
        distinct teachers.teacher_id,
	    usi.school_info_id,
	    si.school_id,
        usi.started_at,
        usi.ended_at,
        rank () over (partition by teachers.teacher_id order by school_id, ended_at desc) rnk
    from teachers
    left join user_school_infos usi    
        on usi.user_id = teachers.teacher_id
    left join school_infos si 
        on si.school_info_id = usi.school_info_id
    where school_id is not null
),

teacher_latest_school as (
    select 
        distinct teacher_id,
        school_id
    from teacher_schools
    where rnk = 1
)

select 
    teachers.*, 
    tls.school_id,
    user_geos.is_international
from teachers 
join user_geos 
    on teachers.teacher_id = user_geos.user_id
left join teacher_latest_school tls 
    on tls.teacher_id = user_geos.user_id
