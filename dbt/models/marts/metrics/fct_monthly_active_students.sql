-- Step 0: Stage data 
with 
active_students as (
    select 
        *, 
        date_trunc('month',activity_date) as activity_month 
    from {{ ref('dim_active_students') }}
),

school_years as (
    select * 
    from {{ref('int_school_years') }}
),

first_active_month as (
    select 
        student_id,
        min(activity_month) as first_activity_month 
    from active_students
    group by student_id 
),

combined as (
    select 
        sy.school_year,
        activity_month,
        us_intl,
        country,
        student_id
    from active_students 
    join school_years as sy 
        on active_students.activity_date 
            between sy.started_at
                and sy.ended_at 
),

final as (
    select 
        com.school_year,
        com.activity_month,
        com.us_intl,
        com.country,
        count(distinct com.student_id) as num_active_students,
        count(distinct case 
            when com.activity_month = fam.first_activity_month 
            then fam.student_id end)  as num_active_students_ytd
    
    from combined                   as com
    left join first_active_month    as fam 
        on com.student_id = fam.student_id 
        and com.activity_month = fam.first_activity_month
    
    {{ dbt_utils.group_by(4) }} 
),

rolling_final as (
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
            rows between unbounded preceding 
                     and current row) as num_active_students_ytd 
    from final 
    {{ dbt_utils.group_by(5) }} )

select * 
from rolling_final
order by activity_month desc 