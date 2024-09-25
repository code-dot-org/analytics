{# 
    model: fct_active_students_monthly
    changelog:
    author      version date        comments
    js          2.0    2024-09-17   init 
    ""          2.1     ""          removing anonymous users from scope
#}

with 
users as (
    select * 
    from {{ ref('dim_users') }}
    where user_type = 'student'
),

-- 1. combine user_levels, sign_ins, and projects data 

user_levels as (
    select  

        user_id             as user_id,
        created_date        as activity_date,
        1               as has_user_level_activity,
        null            as has_sign_in_activity,
        null            as has_project_activity
        
    from {{ ref('dim_user_levels') }}
    where 
        created_date > {{ get_cutoff_date() }}
        and total_attempts > 0 
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
        sign_in_date > {{ get_cutoff_date() }}
        and num_sign_ins > 0
), 

projects as (
    select 

        user_id,
        project_created_at::date    as activity_date,
        null                        as has_user_level_activity,
        null                        as has_sign_in_activity,
        1                           as has_project_activity

    from {{ ref('dim_student_projects') }}
    where project_created_at > {{ get_cutoff_date() }}
        and user_type = 'student'
        and project_info = 1 
),

unioned as (
    select * from user_levels
    union all 
    select * from sign_ins 
    union all 
    select * from projects 
),

combined as (
    select 
        date_trunc('month', uni.activity_date)  as activity_month,
        uni.user_id                             as student_id,
        
        usr.us_intl,
        usr.country,

        max(uni.has_user_level_activity)       as has_user_level_activity,
        max(uni.has_sign_in_activity)          as has_sign_in_activity,
        max(uni.has_project_activity)          as has_project_activity

    from unioned    as uni
    join users      as usr
        on uni.user_id = usr.user_id

    {{ dbt_utils.group_by(4) }} 
), 

final as (
    select
        activity_month,
        us_intl,
        country,
        
        count(distinct student_id) as num_active_students
        {#  Active Student Metric: 
            
            1.  Any user_id with a sign_in on any given day 
                
                AND
                
            2.a. an attempted user_level activity 
                OR
            
            2.b. A `projects` row exists 

            3.a A student is defined as a known CDO user (non-anonymous data)

        -- js; 20240920                         #}
        from combined 
        
        where   has_sign_in_activity = 1
                and coalesce(
                    has_project_activity,
                    has_user_level_activity) = 1
        
        {{ dbt_utils.group_by(3) }} )

select *
from final 
order by activity_month desc  