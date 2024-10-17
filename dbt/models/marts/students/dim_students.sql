with 
students as (
    select *    
    from {{ ref('dim_users')}}
    where user_type = 'student'
),

school_years as (
    select * 
    from {{ref('int_school_years')}}
),

school_association as (
    select 
        student_id,
        school_id,
        row_number() over (
            partition by 
                student_id 
            order by 
                school_year desc)   as row_num

    from {{ref('int_section_mapping')}}
    where school_id is not null
),

final as (
    select 
        -- student info 
        students.user_id    as student_id,
        sa.school_id,
        sy.school_year      as created_at_school_year, 
        students.is_urg,
        students.gender_group,
        students.race_group,
        students.birthday,
        students.locale,
        students.country,
        students.us_intl,

        -- aggs 
        students.sign_in_count,
        students.total_lines,
        
        -- dates
        students.current_sign_in_at,
        students.last_sign_in_at,
        students.created_at,
        students.updated_at,  
        students.deleted_at,   
        students.purged_at,
        students.cap_status,
        students.cap_status_date

    from students 
    left join school_years  as sy 
        on students.created_at 
            between sy.started_at 
                and sy.ended_at 
    left join school_association    as sa 
        on sa.student_id = students.user_id
        and sa.row_num = 1 )

select * 
from final 
