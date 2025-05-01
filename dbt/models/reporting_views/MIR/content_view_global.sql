/* Edit log

- CK 2025-4-29 - fixing an issue with small countries not having a value for all months and courses
*/
  
    -- Step 0: Stage data
    with 
    students_by_curriculum as (
        select 
            student_id, 
            coalesce(course_name,'NULL') as course_name, --wouldn't normally do this, but NULL values are making the partition step fail
            school_year,
            coalesce(country,'NULL') as country,
            date_trunc('month', activity_date) as activity_month 
        from {{ ref('dim_student_script_level_activity') }}
        where
            user_type = 'student'
            and us_intl = 'intl'
    )

, countries as (
    select distinct country
    from students_by_curriculum
)

, months as (
    select distinct activity_month
    from students_by_curriculum
)

, courses as (
    select distinct course_name
    from students_by_curriculum
)

, school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

, crossjoin as (
    select
        country,
        activity_month,
        course_name,
        school_year
    from countries
    cross join months
    cross join courses
    inner join school_years 
        on months.activity_month
            between school_years.started_at 
            and school_years.ended_at
    order by 1,2
)
    
, first_active_month as (
    select 
        student_id, 
        course_name,
        school_year,
        country,
        min(activity_month) as first_activity_month 
    from students_by_curriculum
    group by student_id, school_year, course_name, country
)

, combined_student as (
    select 
        sc.student_id,
        sc.course_name,
        sc.country,
        sc.activity_month,
        sc.school_year,
        fam.first_activity_month as first_activity_month
    from students_by_curriculum as sc 
    left join first_active_month as fam 
         on sc.student_id = fam.student_id 
        and sc.school_year = fam.school_year
        and sc.course_name = fam.course_name
        and sc.country = fam.country
        --and sc.activity_month >= fam.first_activity_month
)

, aggregate as (
    select 
        course_name,
        school_year,
        activity_month,
        country,
        count(distinct student_id) as num_active_students,
        count(distinct case 
            when activity_month = first_activity_month
            then student_id end) as num_new_students
    from combined_student as com
    group by 1,2,3,4
)

, rolling_final_prep as (
    select 
        crossjoin.activity_month,
        crossjoin.country,
        crossjoin.course_name,
        crossjoin.school_year,
        coalesce(num_active_students,0) as num_active_students,
        coalesce(num_new_students,0) as num_new_students
    from crossjoin
    left join aggregate
        on aggregate.country = crossjoin.country
        and aggregate.activity_month = crossjoin.activity_month
        and aggregate.course_name = crossjoin.course_name
)

, rolling_final as (
    select 
        school_year,
        activity_month,
        country,
        course_name,
        num_active_students,
        sum(num_new_students) over (
            partition by 
                school_year, 
                country,
                course_name
            order by activity_month
            rows between unbounded preceding and current row
        ) as num_ytd_students
    from rolling_final_prep )

select * 
from rolling_final
order by 
    school_year desc, 
    country,
    course_name,
    activity_month