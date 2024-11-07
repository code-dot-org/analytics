-- model: dim_active_students 

with 

students as (
    select * 
    from {{ref('dim_students')}}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

dssla as (
    select  
        student_id,
        activity_date,
        1               as has_user_level_activity,
        0               as has_project_activity,
        course_name,
        school_state
        
    from {{ ref('dim_student_script_level_activity') }}
    where 
        total_attempts > 0 
        and created_date > {{ get_cutoff_date() }} 
        and user_type = 'student'
), 

projects as (
    select 
        user_id                     as student_id,
        project_created_at::date    as activity_date,
        0                           as has_user_level_activity
        1                           as has_project_activity,
        NULL as course_name,
        NULL as school_state

    from {{ ref('dim_student_projects') }}
    where 
        user_type = 'student'
        and project_created_at > {{ get_cutoff_date() }} 
),

unioned as (
    select * from user_levels
    union all 
    select * from projects 
), 

combined as (
    unioned.student_id,
    sy.school_year,
    unioned.activity_date,
    extract('month' from created_date)  as activity_month,

    case
        when course_name = 'csf' then 1
        else 0
    end as csf,
    case
        when course_name = 'csd' then 1
        else 0
    end as csd,
    case
        when course_name = 'hoc' then 1
        else 0
    end as hoc,
    case
        when course_name = 'csp' then 1
        else 0
    end as csp,
    case
        when course_name = 'other' or course_name is null then 1
        else 0
    end as other,
    case
        when course_name = 'csc' then 1
        else 0
    end as csc,
    case
        when course_name = 'csa' then 1
        else 0
    end as csa,
    case
        when course_name = 'ai' then 1
        else 0
    end as ai,
    case
        when course_name = 'foundations of cs' then 1
        else 0
    end as foundations_cs,
    case
        when course_name = '9-12 special topics' then 1
        else 0
    end as special_topics,

    left join school_years as sy 
        on uni.activity_date 
            between sy.started_at
                and sy.ended_at 

)


combined2 as (
    select 
        combined.student_id,
        combined.school_year,
        combined.activity_date,
        combined.activity_month,

        students.country,
        students.gender_group,
        students.race_group,

        max(combined.csa)       as has_csa,
        max(combined.csp)       as has_csp,
        max(combined.csd)       as has_csd,
        max(combined.ai)        as has_ai,
        max(combined.csf)       as has_csf,
        max(combined.foundations)   as has_foundations,
        max(combined.hoc)           as hoc,
        max(combined.other)         as other,
        
        max(uni.has_project_activity)          as has_project_activity

    from combined
    left join students
        on combined.student_id = combined2.student_id

    {{ dbt_utils.group_by(7) }} 
),

final as (
    select 
        *, 
        1 as is_active_student

    from combined 
    where coalesce(
            has_project_activity,
            has_user_level_activity) = 1 )

select * 
from final
