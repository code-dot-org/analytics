{# 
    model: fct_active_students_monthly
    changelog:
    author      version date        comments
    ck          1.0    2024-10-17   init 
    
#}

with active_students as (
    select * 
    from {{ ref('dim_student_script_level_activity') }}
    where user_type = 'student'
    and is_international = 0
),

urg_data as (
    select * 
    from {{ref('dim_users')}}
),

district_data as (
    select * 
    from {{ref('dim_district_status')}}
),

merged as (
    select active_students.student_id, 
    active_students.school_year, 
    activity_date,
    course_name,
    active_students.school_id,
    school_status,
    school_state,
    urg_data.race_group as race,
    urg_data.gender_group as gender,
    district_data.enrolled as enrolled
    from active_students
    left join urg_data 
    on active_students.student_id = urg_data.student_id
    left join district_data
    on active_students.school_district_id = district_data.school_district_id 
    and active_students.school_year = district_data.school_year
),

final as (
    select 
        date_trunc('month',activity_date) as activity_month,
        school_state,
        enrolled,
        course_name,
        race,
        gender,
        count(distinct student_id) as num_active_students
    from merged
    {{ dbt_utils.group_by(6) }} )
    
select * 
from final 
order by activity_month desc, school_state desc, course_name desc, enrolled 