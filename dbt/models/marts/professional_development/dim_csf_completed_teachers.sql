with
script_names as (
    select * 
    from {{ ref('dim_script_names') }}
),

scripts as (
    select * 
    from {{ ref('stg_dashboard__scripts') }}
),

followers as (
    select * 
    from {{ ref('stg_dashboard__followers') }}
),

sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
),

users as (
    select * 
    from {{ ref('stg_dashboard__users') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

csf_stages_for_completion as (
    select * 
    from {{ ref('seed_csf_stages_for_completion') }}
),

csf_plugged_stage_counts as (
    select * 
    from {{ ref('seed_csf_plugged_stage_counts') }}
),

user_stages as (
    select * 
    from {{ ref('dim_user_stages') }}
),

csf_temp as (
    select 
    us.user_id, 
    coalesce(sn.script_name_short, sc.script_name)                                                 as script_name, 
    us.script_id, 
    us.stage_id, 
    sy.school_year, 
    us.stage_started_at, 
    row_number() over(partition by us.user_id, us.script_id order by us.stage_started_at asc)     as stage_order
  from user_stages us
    join scripts sc on sc.script_id = us.script_id
    left join script_names sn on sn.versioned_script_id = sc.script_id
    join school_years sy on us.stage_started_at between sy.started_at and sy.ended_at
    join users u on u.user_id = us.user_id and u.user_type = 'student'
    join csf_stages_for_completion sfc on sfc.script_id = us.script_id and sfc.stage_number = us.stage_number
),

csf_completed as (
    select 
    csf_temp.user_id, 
    csf_temp.script_id, 
    csf_temp.script_name,
    csf_temp.school_year,
    csf_temp.stage_started_at as completed_at -- starting the Nth stage (dependent on script) representing "completing" the course
    from csf_temp
    join csf_plugged_stage_counts sc on sc.script_id = csf_temp.script_id and csf_temp.stage_order = sc.plugged_stage_counts
),

csf_completed_temp as (
    select 
    se.user_id,     -- why are you using the section's user_id here? (btw its all 1:1 joins so they're all the same :) )
    com.school_year,
    com.script_id,
    com.script_name,
    completed_at,
    row_number() over(partition by se.user_id, com.school_year order by completed_at asc) completed_at_order
  from csf_completed com
    join school_years sy on com.completed_at between sy.started_at and sy.ended_at
    join followers f on f.student_user_id = com.user_id and f.created_at between sy.started_at and sy.ended_at
    join sections se on se.section_id = f.section_id
)

select 
    user_id,
    school_year, 
    script_id,
    script_name,
    completed_at
from csf_completed_temp
where completed_at_order = 5 -- what does this mean? the 5th completed section for any given school year? 


-- next steps: decomp with @allison-code-dot-org on how to reconcile these two models
