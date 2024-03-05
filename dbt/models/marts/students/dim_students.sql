with 
students as (
    select {{ dbt_utils.star(
        ref('dim_users'), 
        except=['teacher_email']) }}
    from {{ ref('dim_users') }}
    where user_type = 'student'
),

school_years as (
    select * 
    from {{ref('int_school_years')}}
),

final as (
    select 
        students.user_id,
        students.is_urg,
        students.is_international,
        students.us_intl,
        students.race_group,
        students.gender_group, 
        sy.school_year created_at_school_year
    from students 
    left join school_years sy 
        on students.created_at 
            between sy.started_at 
            and sy.ended_at)

select * 
from final 