-- re: dataops-515

with 
school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

student_courses as (
    select 
        user_id,
        school_year,
        course_name
    from {{ ref('dim_user_course_activity') }}
),

combined as (
    select 
        sc.user_id,
        sy.school_year,
        sc.course_name,
        case when sc.user_id is not null then 1 else 0 end as is_active
    from student_courses    as sc 
    left join school_years  as sy 
        on sc.school_year = sy.school_year
),

aggregated as (
    select 
        user_id,
        school_year,
        is_active,
        lag(is_active) over (
            partition by user_id order by school_year) as is_active_previous_year,
        max(is_active) over (
            partition by user_id) as is_active_all_years,
        listagg(distinct course_name, ', ') 
            within group (order by course_name asc) as courses_started
    from combined 
    {{ dbt_utils.group_by(3) }}
),

final as (
    select 
        user_id as student_id,
        school_year, 
        courses_started,

        case when is_active 
             and not is_active_previous_year 
            then 'active - new student'
             
            when is_active 
             and is_active_previous_year 
            then 'active - returning student'

            when not is_active 
             and is_active_previous_year 
            then 'inactive - former student'
            
            when not is_active 
             and not is_active_previous_year 
             and is_active_all_years
            then 'inactive - churned'

            when not is_active_all_years then 'market'

            else (is_active || is_active_previous_year || is_active_all_years)
        end as student_status

    from aggregated)

select * 
from final 
