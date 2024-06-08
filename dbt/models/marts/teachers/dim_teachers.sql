with 
teachers as (
    select *
    from {{ ref('dim_users')}}
    where user_type = 'teacher'
),

section_instructors as (
    select * 
    from {{ ref('stg_dashboard__section_instructors')}}
),

user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
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

-- get teacher NCES school_id association
teacher_schools as (
    select 
        teachers.user_id,
        si.school_id,
        rank () over (
            partition by teachers.user_id 
            order by usi.ended_at desc) as rnk
    from teachers
    left join user_school_infos as usi    
        on usi.user_id = teachers.user_id
    left join school_infos as si 
        on si.school_info_id = usi.school_info_id
),

final as (
    select 
        tea.*, 
        sei.is_section_owner,
        ts.school_id,
        school_years.school_year as created_at_school_year
    from teachers as tea
    left join section_instructors as sei 
        on tea.teacher_id = sei.teacher_id 
        and tea.section_id = sei.section_id 
    inner join teacher_schools as ts 
        on tea.user_id = ts.user_id
        and ts.rnk = 1
    inner join school_years 
        on tea.created_at 
            between school_years.started_at 
                and school_years.ended_at)

select * 
from final