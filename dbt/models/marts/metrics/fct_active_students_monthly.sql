-- model: fct_active_students_monthly
-- version 2.0 (js; 2024-09-17)

with 
user_levels as (
    select  
        created_date as activity_date
        user_id as student_id,
        1 as is_active,
        1 as has_user_level_activity
    from {{ ref('dim_user_levels') }}
    where created_date > {{ get_cutoff_date() }}
        and total_attempts > 0 
), 

sign_ins as (
    select 
        sign_in_date as activity_date,
        user_id as student_id,
        1 as is_active, 
        1 as has_sign_in_activity
    from {{ ref('dim_user_sign_ins') }}
    where sign_in_date > {{ get_cutoff_date() }}
        and num_sign_ins > 0
), 

projects as (
    select 
        project_created_at::date as activity_date,
        user_id as student_id,
        -- known_cdo_user, -- includes anonymous users (0=anon, 1=known)
        1 as is_active,
        1 as has_project_activity

    from {{ ref('dim_student_projects') }}
    where project_created_at > {{ get_cutoff_date() }}
), -- select * from projects 

combined as (
    select
    activity_date,
    count(distinct 
    case when has_sign_in_activity = 1
    and coalesce(has_user_level_activity,has_project_activity)=1
    then student_id end) as num_active_students
    from ... 




    /*
    select 'User Levels'                    as activity_type,
        date_trunc('month',activity_date)   as activity_month,
        country, 
        us_intl,
        count(distinct user_id)             as num_active_students
    from user_levels
    {{ dbt_utils.group_by(4) }}
    
    union all

    select 'Sign Ins',
        date_trunc('month',activity_date),
        country,
        us_intl,
        count(distinct user_id)
    from sign_ins
    {{ dbt_utils.group_by(4) }}

    union all

    select 'Projects' as activity_type,
        date_trunc('month',activity_date),
        country,
        us_intl,
        count(distinct user_id)
    from projects
    {{ dbt_utils.group_by(4) }}
) select * from combined ;

final as (
    select 
        activity_type,
        activity_month,
        country,
        us_intl,
        sum(num_actives) as num_actives
    from combined
    {{ dbt_utils.group_by(4)}} )

select * 
from final
order by activity_month desc 