-- model: dim_active_students
-- scope: active student user activity 
-- range: 22-23 school year to current 
-- auth: bfranke

with 
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels')}}
),

sign_ins as (
    select *
    from {{ ref('stg_dashboard__sign_ins') }}
),

projects as (
    select *
    from {{ ref('stg_dashboard_pii__projects') }}
),

project_storage_id as (
    select distinct 
        user_id,
        user_project_storage_id
    from {{ ref('stg_dashboard__user_project_storage_ids') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

users as (
    select * 
    from {{ ref('dim_users') }}
    where user_type = 'student'
),

project_activity_summary as (
    select 
        coalesce(
            upsi.user_id::varchar,
            pro.storage_id::varchar)        as user_id,
        pro.created_at::date                as activity_date,
        case 
            when upsi.user_id is not null 
            then 1 else 0 end               as is_known_cdo_user,
        count(distinct pro.project_id)      as num_projects --number of projects
    from projects as pro 
    left join project_storage_id as upsi
        on pro.storage_id = upsi.user_project_storage_id
    {{ dbt_utils.group_by(3) }}
),

user_level_summary as (
    select
        user_id,
        1                               as is_known_cdo_user,
        sign_in_at::date                as activity_date,
        count(distinct user_level_id)   as num_user_levels -- number of user_level rows 
    from user_levels 
    {{ dbt_utils.group_by(3) }}
),

sign_in_summary as (
    select 
        user_id,
        1                           as is_known_cdo_user,
        sign_in_at::date            as activity_date,
        count(distinct sign_in_at)  as num_sign_ins -- number of sign_ins
    from sign_ins
    {{ dbt_utils.group_by(3) }}
),

aggregated as (
    select 
        user_id, 
        is_known_cdo_user, 
        activity_date, 
        num_projects,
        null as num_user_levels,
        null as num_sign_ins
    from project_activity_summary
    union all

    select 
        user_id, 
        is_known_cdo_user, 
        activity_date, 
        null as num_projects,
        num_user_levels,
        null as num_sign_ins
    from user_level_summary
    union all 
    
    select 
        user_id, 
        is_known_cdo_user, 
        activity_date, 
        null as num_projects,
        null as num_user_levels,
        num_sign_ins 
    from sign_in_summary
),

combined as (
    select 
        user_id,
        activity_date,
        max(is_known_cdo_user)  as is_known_cdo_user,
        sum(num_projects)       as num_projects,
        sum(num_user_levels)    as num_user_levels,
        sum(num_sign_ins)       as num_sign_ins
    from aggregated
    {{ dbt_utils.group_by(2) }}
),

final as (
    select 
        comb.user_id,
        case 
            when comb.is_known_cdo_user >= 1 
                then usr.user_type 
            else 'anonymous' 
        end as user_type,

        comb.activity_date,
        extract('year' from comb.activity_date) as calendar_year,
        sy.school_year,
        
        usr.country,
        usr.us_intl,
        usr.school_id,
        usr.grade_level,

        sum(comb.num_projects)      as num_projects,
        sum(comb.num_user_levels)   as num_user_levels,
        sum(comb.num_sign_ins)      as num_sign_ins
        
    from combined as comb
    join users as usr
        on comb.user_id = usr.user_id
    join school_years as sy 
        on comb.activity_date 
            between sy.start_date 
                and sy.end_date 
    {{ dbt_utils.group_by(9) }} )

select * 
from final 
-- where activity_date >= '2022-01-01'

