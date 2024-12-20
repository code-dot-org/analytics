    -- Step 0: Stage data
    with 
    students_by_curriculum as (
        select 
            student_id, 
            coalesce(course_name,'NULL') as course_name, --wouldn't normally do this, but NULL values are making the partition step fail
            coalesce(school_state,'NULL') as school_state,
            school_year,
            date_trunc('month', activity_date) as activity_month 
        from {{ ref('dim_student_script_level_activity') }}
        where
            user_type = 'student' 
            and us_intl = 'us'
    )
    
, first_active_month as (
    select 
        student_id, 
        course_name,
        school_state,
        school_year,
        min(activity_month) as first_activity_month 
    from students_by_curriculum
    group by student_id, school_year, course_name, school_state
)

, combined as (
    select 
        sc.student_id,
        sc.course_name,
        sc.school_state,
        sc.school_year,
        sc.activity_month,
        fam.first_activity_month as first_activity_month
    from students_by_curriculum as sc 
    left join first_active_month as fam 
         on sc.student_id = fam.student_id 
        and sc.school_year = fam.school_year
        and sc.course_name = fam.course_name
        and sc.school_state = fam.school_state
        --and sc.activity_month >= fam.first_activity_month
)

, final as (
    select 
        course_name,
        school_year,
        activity_month,
        school_state,
        count(distinct student_id) as num_active_students,
        count(distinct case 
            when activity_month = first_activity_month
            then student_id end) as num_new_students
    from combined as com
    group by 1,2,3,4
)

, rolling_final_prep as (
    select 
        school_year,
        activity_month,
        school_state,
        course_name,
        num_active_students,
        num_new_students
    from final
)

, rolling_final as (
    select 
        school_year,
        activity_month,
        school_state,
        course_name,
        num_active_students,
        sum(num_new_students) over (
            partition by 
                school_year, 
                school_state,
                course_name
            order by activity_month
            rows between unbounded preceding and current row
        ) as num_active_students_ytd
    from rolling_final_prep )

select * 
from rolling_final
order by 
    school_year desc, 
    activity_month desc,
    course_name,
    school_state 
