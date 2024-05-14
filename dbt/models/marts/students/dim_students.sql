with 
students as (
    select *    
    from {{ ref('dim_users')}}
    where user_type = 'student'
),

school_years as (
    select * from {{ref('int_school_years')}}
),

section_mapping as (
    select * 
    from {{ref('int_section_mapping')}}
),

final as (
    select 
        students.*, 
        sm.school_id,
        sy.school_year as created_at_school_year
    from students 
    left join school_years as sy 
        on students.created_at 
            between sy.started_at 
                and sy.ended_at
    left join section_mapping as sm 
        on students.student_id = sm.student_id
        and students.school_info_id = sm.school_info_id)

select * 
from final 