-- model: fct_active_students_monthly
-- version 2.0 (js; 2024-09-17)

with cutoff_date as (select '2020-06-30'::date as cutoff_date),

user_levels as (
    select  
        created_date, 
        user_id,
        1 as is_active
    from {{ ref('dim_user_levels') }}
    where created_date > (select cutoff_date from cutoff_date)
        and total_attempts > 0 
), 

sign_ins as (
    select 
        sign_in_date,
        user_id,
        1 as is_active
    from {{ ref('dim_user_sign_ins') }}
    where sign_in_date > (select cutoff_date from cutoff_date)
        and num_sign_ins > 0
), 

projects as (
    select 
        project_created_at::date 
        user_id,
        known_cdo_user, -- includes anonymous users (0=anon, 1=known)
        1 as is_active

    from {{ ref('dim_student_projects') }}
    where project_created_at > (select cutoff_date from cutoff_date)
    
    {{ dbt_utils.group_by(6) }}
    having num_rows > 0 
), -- select * from projects 

combined as (
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