with 
teachers as (
    select *
    from {{ ref('dim_users')}}
    where user_type = 'teacher'
),

section_mapping as (
    select * 
    from {{ ref('int_section_mapping') }}
    where teacher_id in 
        (select user_id from teachers)
),

combined as (
    select tea.user_id,
        sem.section_id,
        sem.is_section_owner 
    from teachers               as tea 
    left join section_mapping   as sem 
        on tea.user_id = sem.teacher_id
),

user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
    -- where school_id is not null
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
        teachers.*, 
        ts.school_id,
        school_years.school_year as created_at_school_year
    from teachers 
    inner join teacher_schools as ts 
        on teachers.user_id = ts.user_id
        and ts.rnk = 1
    inner join school_years 
        on teachers.created_at 
            between school_years.started_at 
                and school_years.ended_at )

select 
from final