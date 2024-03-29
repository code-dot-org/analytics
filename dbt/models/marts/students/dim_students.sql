with 
students as (
     select 
        {{ dbt_utils.star(from=ref('dim_users'), 
            except=[
                "user_id", 
                "user_type", 
                "teacher_id",
                "teacher_email"]) }}
    from {{ ref('dim_users') }}
    where user_type = 'student'
),

school_years as (
    select * 
    from {{ref('int_school_years')}}
),

final as (
    select 
        -- users
        students.student_id,
        students.studio_person_id,
        students.school_info_id,
        students.is_urg,
        students.gender,
        students.locale,
        students.birthday,
        students.sign_in_count,
        students.total_lines,     
        students.races,
        students.race_group,
        students.gender_group,
        students.is_international,
        students.us_intl,
        students.country,

        -- dates
        sy.school_year as created_at_school_year,

        students.current_sign_in_at,
        students.last_sign_in_at,
        students.created_at,
        students.updated_at
        
    from students 
    left join school_years sy 
        on students.created_at 
            between sy.started_at 
                and sy.ended_at
)

select * 
from final 