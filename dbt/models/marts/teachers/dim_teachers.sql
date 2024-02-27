with 
teachers as (
    select *
    from {{ ref('dim_users')}}
    where user_type = 'teacher' 
),

user_school_infos as (
    select user_id, school_id
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
    where user_id in (select user_id from teachers)
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
    where school_id is not null 
), 

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

coteachers as (
    select 
        instructor_id, 
        1 as is_coteacher
    from {{ ref('dim_section_instructors') }}
    where instructor_id in (select user_id from teachers)
),

-- get teacher NCES school_id association
teacher_schools as (
    select 
        teachers.user_id,
        usi.school_id,
        rank () over (
            partition by teachers.user_id 
            order by usi.ended_at desc) rnk
    from teachers
    left join user_school_infos as usi    
        on usi.user_id = teachers.user_id
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

        -- teacher info 
        teachers.user_id, 
        teachers.teacher_email,
        
        tls.school_id,
        teachers.country,
        {# teachers.is_international, #}
        teachers.us_intl,

        -- very vague init definition for is_coteacher...
        coalesce(coteachers.is_coteacher,0) as is_coteacher,

        -- metadata 
        school_years.school_year as created_at_school_year,
        teachers.created_at,
        teachers.updated_at
        
    from teachers 
    left join teacher_latest_school tls 
        on teachers.user_id = tls.user_id
    left join school_years 
        on teachers.created_at 
            between school_years.started_at 
                and school_years.ended_at
    left join coteachers 
        on teachers.user_id = coteachers.instructor_id)
     
select *
from final 