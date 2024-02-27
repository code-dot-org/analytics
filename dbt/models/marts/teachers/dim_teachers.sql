with 
teachers as (
    select * 
    from {{ ref('dim_users')}}
    where user_type = 'teacher' 
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

coteachers as (
    select * 
    from {{ ref('stg_dashboard__section_instructors') }}
),



-- get teacher NCES school_id association
teacher_schools as (
    select 
        teachers.user_id,
        si.school_id,
        rank () over (
            partition by teachers.user_id 
            order by si.school_id, usi.ended_at desc) rnk
    from teachers
    left join user_school_infos as usi    
        on usi.user_id = teachers.user_id
    left join school_infos as si 
        on si.school_info_id = usi.school_info_id
    where si.school_id is not null
),

teacher_latest_school as (
    select 
        user_id,
        school_id
    from teacher_schools
    where rnk = 1
),

final as (    
    select 
        teachers.*, 
        tls.school_id,
        school_years.school_year as created_at_school_year
    from teachers 
    left join teacher_latest_school tls 
        on teachers.user_id = tls.user_id
    left join school_years 
        on teachers.created_at 
            between school_years.started_at and school_years.ended_at)

select *
from final 