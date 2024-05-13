/*
    This model represents the ** prototype of the active student metric ** presented by Baker to LT on May. 9, 2024

    This model can get really big - but can be optimized further.

    All 3 CTEs consult large tables (user_levels, projects, sign_ins)

    1) each of these CTEs - which summarize daily activity per user - might be usefual in their own right as an (intermediate?) table.
    2) These might be able to leverage DBT's incremental modeling

    I have used a cutoff date of anything after 2022-07-01, the start of the 22-23 school year.
*/

-- daily user_level activity summmary
with cutoff_date as (
    select '2022-07-01'::date as cutoff_date -- use this as a cutoff date for all CTEs.
)
, ul_summary as (
    select
    
        ul.user_id,
        ul.created_at::date activity_date,
        --ul.updated_at::date       -- would be better if we could log updated_at for daily acitivity

        max(ul.updated_at)::date most_recent_updated_at,
        listagg(distinct cs.course_name_true) within group (order by cs.course_name_true) courses,

        count(*) num_user_level_records
    
    from {{ ref('stg_dashboard__user_levels') }} ul
    left join {{ ref('dim_course_structure') }} cs 
        on cs.level_id = ul.level_id 
        and cs.script_id = ul.script_id
    where ul.created_at >= (select cutoff_date from cutoff_date limit 1) 
    group by 1,2
)
-- daily sign-ins summary
, sign_in_summary as (
    select
        user_id,
        sign_in_at::date activity_date,
        count(*) num_sign_ins
    from {{ ref('stg_dashboard__sign_ins') }}
    where sign_in_at::date >= (select cutoff_date from cutoff_date limit 1) 
    group by 1,2
)
-- daily project summary
, projects_summary as (
    select
        upsi.user_id,
        created_at::date activity_date,
        listagg(distinct project_type, ', ') within group (order by project_type) as project_types,
        count(*) num_project_records
        
    from {{ ref('stg_dashboard_pii__projects') }} p
    left join {{ ref('stg_dashboard__user_project_storage_ids') }} upsi on upsi.user_project_storage_id = p.storage_id
    where created_at >= (select cutoff_date from cutoff_date limit 1) 
    group by 1,2
)
-- Do a full outer join across user_levels, sign_ins, and projects
select 
    coalesce(p.activity_date, ul.activity_date, si.activity_date) AS activity_at_merged,
    coalesce(p.user_id, ul.user_id, si.user_id) AS user_id_merged,
    u.user_type,
    ug.country,
    ug.us_intl,
    sy.school_year,
    p.num_project_records,
    p.project_types,
    --ul.num_user_level_records,
    --ul.num_scripts,
    ul.courses,
    si.num_sign_ins,  

    -- some useful flags
    case when si.user_id IS NOT NULL then 1 else 0 end has_sign_in_activity,
    case when ul.user_id IS NOT NULL then 1 else 0 end has_user_level_activity,
    case when p.user_id IS NOT NULL then 1 else 0 end has_project_activity,

    -- this can be used for a complete segmentation across all activity types.
    (case when num_user_level_records IS NOT NULL then 'L' else '_' end ||
    case when num_project_records IS NOT null then 'P' else '_' end ||
    case when num_sign_ins IS NOT NULL then 'S' else '_' END) as activity_type

FROM projects_summary p
FULL OUTER JOIN 
    ul_summary ul 
    ON p.user_id = ul.user_id AND p.activity_date = ul.activity_date
FULL OUTER JOIN 
    sign_in_summary si 
    ON coalesce(p.user_id, ul.user_id) = si.user_id 
    AND coalesce(p.activity_date, ul.activity_date) = si.activity_date
LEFT JOIN {{ ref('stg_dashboard__users') }} u ON u.user_id = coalesce(p.user_id, ul.user_id, si.user_id)
LEFT JOIN {{ ref('stg_dashboard__user_geos') }} ug ON ug.user_id = coalesce(p.user_id, ul.user_id, si.user_id)
LEFT JOIN {{ ref('int_school_years') }} sy on coalesce(p.activity_date, ul.activity_date, si.activity_date) between sy.started_at and sy.ended_at
