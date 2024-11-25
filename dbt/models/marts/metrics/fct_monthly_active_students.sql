{# 
    model: fct_active_students_monthly
    changelog:
    author      version date        comments
    js          2.0    2024-09-17   init 
    ""          2.1     ""          removing anonymous users from scope
#}

with active_students as (
    select * 
    from {{ ref('dim_active_students') }}
),

school_years as (
    select * 
    from {{ref('int_school_years') }}
),

combined as (
    select 
        sy.school_year,
        date_trunc('month', activity_date) as activity_month,
        us_intl,
        country,
        count(distinct student_id) as num_active_students
    from active_students 
    join school_years as sy 
        on active_students.activity_date 
            between sy.started_at
                and sy.ended_at 
    {{ dbt_utils.group_by(4) }}
),

final as (
    select 
        school_year,
        activity_month,
        us_intl,
        country,
        num_active_students,
        sum(num_active_students) over(
            partition by 
                school_year,
                us_intl,
                country
            order by activity_month
            rows between unbounded preceding and current row
        ) as num_active_students_ytd 
    from combined )
    
select * 
from final 