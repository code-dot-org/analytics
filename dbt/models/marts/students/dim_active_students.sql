/*
    This model represents the ** prototype of the active student metric ** presented by Baker to LT on May. 9, 2024

    This model can get really big -- all 3 CTEs consult large tables (user_levels, projects, sign_ins).
    
    It can also definitely be optimized later.
    1) Each of these CTEs - which summarize daily activity per user - might be usefual in their own right as an (intermediate?) table.
    2) These might be able to leverage DBT's incremental modeling

    I have used a cutoff date of anything after 2022-07-01, the start of the 22-23 school year.
*/

with cutoff_date as (
    select '2020-07-01'::date as cutoff_date -- use this as a cutoff date for all CTEs, modify if nec.
)
-- make CTEs for user_level, sign-in and project summaries to set fields for a union
, ul_summary as ( 
    select
        'L'                     as event_type,
        user_id::varchar, 
        1                       as known_cdo_user, --all user_level records have a known user
        activity_date, 
        num_user_level_records  as num_records

    from {{ref("int_daily_summary_user_level_activity")}} ul
    where ul.activity_date >= (select cutoff_date from cutoff_date limit 1) and
    (ul.course_list LIKE '%csf%' or ul.course_list LIKE '%csd%' or ul.course_list LIKE '%csp%' or ul.course_list LIKE '%csa%' or ul.course_list LIKE '%csc%')
    
)
, sign_in_summary as (
    select 
        'S'                 as event_type,
        user_id::varchar, 
        1                   as known_cdo_user, --all sign_in records have a known user
        activity_date, 
        num_sign_ins        as num_records

    from {{ref('int_daily_summary_sign_in')}}
    where activity_date >= (select cutoff_date from cutoff_date limit 1)
)
, projects_summary as (
    select 
        'P'                 as event_type,
        user_id_merged      as "user_id",
        known_cdo_user,      -- projects can have anonymous users
        activity_date, 
        num_project_records as num_records

    from {{ ref('int_daily_summary_project_activity') }}
    where activity_date >= (select cutoff_date from cutoff_date limit 1)
)
, summary_metrics_union as (
    select * from ul_summary
    union all
    select * from sign_in_summary
    union all
    select * from projects_summary

)
-- Aggregate all events by user_id | day
, final as (
    select 
        sm.user_id,
        sm.activity_date,
        u.country,
        u.us_intl,
        sy.school_year,
        extract(year from activity_date) calendar_year,

        case when sum(sm.known_cdo_user) >= 1 then max(u.user_type) else 'anon' end as user_type_merged,

        sum (case when sm.event_type = 'S' then num_records else 0 end)  as num_sign_in_records,
        sum (case when sm.event_type = 'L' then num_records else 0 end)  as num_user_level_records,
        sum (case when sm.event_type = 'P' then num_records else 0 end)  as num_project_records,

        case when num_sign_in_records > 0    then 1 else 0 end  as has_sign_in_activity,
        case when num_user_level_records > 0 then 1 else 0 end  as has_user_level_activity,
        case when num_project_records > 0    then 1 else 0 end  as has_project_activity,

        (  case when has_sign_in_activity = 1       then 'S' else '_' end 
        || case when has_user_level_activity = 1    then 'L' else '_' end 
        || case when has_project_activity = 1       then 'P' else '_' end
        ) as activity_type

    from 
        summary_metrics_union sm
        left join {{ ref('dim_users') }} u on u.user_id = sm.user_id 
        left join {{ ref("int_school_years") }} sy on sm.activity_date between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(6)}}
)
select *
from final 
where user_type_merged IS NOT NULL -- this can be null in cases when user creates account and does levels between the time when users table is replicated and user_levels table is replicated which is about ~12 hour window (i.e. they have valid user_level records and user_id, but it's not in our users table yet.  )
and user_type_merged <> 'teacher' -- i.e. keep 'student' and 'anon'
