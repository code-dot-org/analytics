-- model: fct_active_students_monthly
-- version 2.0 (js; 2024-09-17)

with 
user_levels as (
    select  
        activity_date, 
        user_id,
        1 as known_cdo_user, -- all known users
        country, 
        us_intl,

        case when num_user_level_records > 0 
        then 1 else 0 
        end as is_active

    from {{ ref('dim_user_levels') }}
),

sign_ins as (
    select 
        activity_date,
        user_id,
        1 as known_cdo_user, -- all known users
        country,
        us_intl,

        case when total_sign_ins > 0 
        then 1 else 0 
        end as is_active

    from {{ ref('dim_user_sign_ins') }}
),

projects as (
    select 
        activity_date, 
        user_id,
        known_cdo_user, -- includes anonymous users (0=anon, 1=known)
        country,
        us_intl,
        
        case when num_project_records > 0 
        then 1 else 0 
        end as is_active

    from {{ ref('dim_user_projects') }}
),

combined as (
    select 
        date_trunc('month',activity_date)   as activity_month,
        count(distinct 
            case when is_active = 1 
            then user_id end)               as num_actives
    from user_levels
    group by 1
    union all

    select 
        date_trunc('month',activity_date)   as activity_month,
        count(distinct
            case when is_active = 1 
            then user_id end)               as num_actives
    from sign_ins
    group by 1
    union all

    select 
        date_trun('month',activity_date)    as activity_month,
        count(distinct
            case when is_active = 1
            then user_id end)               as num_actives
    from projects
    group by 1
),

final as (
    select 
        activity_month,
        sum(num_actives) as num_actives
    from combined
    group by 1
)