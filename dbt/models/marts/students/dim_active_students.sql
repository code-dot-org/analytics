-- model: dim_active_students 

with 
users as (
    select * 
    from {{ ref('dim_users') }}
    where user_type = 'student'
),

-- 1. combine user_levels, sign_ins, and projects data 
user_levels as (
    select  

        user_id,
        created_date    as activity_date,
        1               as has_user_level_activity,
        null            as has_sign_in_activity,
        null            as has_project_activity
        
    from {{ ref('dim_user_levels') }}
    where 
        total_attempts > 0 
        and created_date > {{ get_cutoff_date() }} 
), 

sign_ins as (
    select 

        user_id,
        sign_in_date        as activity_date,
        null                as has_user_level_activity,
        1                   as has_sign_in_activity,
        null                as has_project_activity

    from {{ ref('dim_user_sign_ins') }}
    where 
        num_sign_ins > 0
        and sign_in_date > {{ get_cutoff_date() }} 

), 

projects as (
    select 

        user_id,
        project_created_at::date    as activity_date,
        null                        as has_user_level_activity,
        null                        as has_sign_in_activity,
        1                           as has_project_activity

    from {{ ref('dim_student_projects') }}
    where 
        user_type = 'student'
        and project_info = 1 
        and project_created_at > {{ get_cutoff_date() }} 

),

unioned as (
    select * from user_levels
    union all 
    select * from sign_ins 
    union all 
    select * from projects 
), 

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        uni.user_id                             as student_id,
        
        usr.us_intl,
        usr.country,

        uni.activity_date,
        sy.school_year,

        max(uni.has_user_level_activity)       as has_user_level_activity,
        max(uni.has_sign_in_activity)          as has_sign_in_activity,
        max(uni.has_project_activity)          as has_project_activity

    from unioned    as uni
    join users      as usr
        on uni.user_id = usr.user_id
    
    join school_years as sy 
        on uni.activity_date 
            between sy.started_at
                and sy.ended_at 

    {{ dbt_utils.group_by(5) }} 
),

final as (
    select *, 1 as is_active_student
    from combined 
    where has_sign_in_activity = 1
        and coalesce(
            has_project_activity,
            has_user_level_activity) = 1 )

select * 
from final