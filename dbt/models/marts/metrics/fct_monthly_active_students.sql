-- Step 0: Stage data
with 
active_students as (
    select 
        *, 
        date_trunc('month', activity_date) as activity_month 
    from {{ ref('dim_active_students') }}
),

first_active_month as (
    select 
        student_id, 
        school_year,
        min(activity_month) as first_activity_month 
    from active_students
    group by student_id, school_year
),

combined as (
    select 
        acs.school_year,
        acs.activity_month,
        acs.us_intl,
        acs.country,
        acs.student_id
    from active_students as acs 
    left join first_active_month as fam 
         on acs.student_id = fam.student_id 
        and acs.school_year = fam.school_year
        and acs.activity_month >= fam.first_activity_month
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
            then com.student_id end) as num_new_students
    from combined as com
    left join first_active_month as fam 
        on com.student_id = fam.student_id 
        and com.school_year = fam.school_year
    group by com.school_year, com.activity_month, com.us_intl, com.country
),

rolling_final_prep as (
    select 
        school_year,
        activity_month,
        us_intl,
        country,
        num_active_students,
        num_new_students
    from final
),

rolling_final as (
    select 
        school_year,
        activity_month,
        us_intl,
        country,
        num_active_students,
        sum(num_new_students) over (
            partition by 
                school_year, 
                us_intl, 
                country
            order by activity_month
            rows between unbounded preceding and current row
        ) as num_active_students_ytd
    from rolling_final_prep )

select * 
from rolling_final
order by 
    school_year, 
    activity_month desc