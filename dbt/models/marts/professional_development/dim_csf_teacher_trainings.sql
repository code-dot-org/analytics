/*
Model: dim_csf_teacher_trainings
Author: JS
Notes:
- combine csf training sub-models into one model that:
- using this archived pr for (reference)[https://github.com/code-dot-org/analytics-infrastructure/pull/63]
    (Acceptance Criteria)
    1. Displays one row for each teacher & school year (uid)
    2. Can be combined with either dim_teachers or dim_csf_workshops (TO DO)
    3. A set of test criteria are desgined to describe common "bad data" outcomes
    e.g. 
        - started_at > completed_at, last_progress_at...
        - UID is not unique 
        - user_id, school_year is missing

    (Proposed)
    select 
        user_id,
        school_year,
        script_id,
        script_name, -- i say we remove this (js)
        started_at,
        last_progress_at,
        completed_at
    from {{ this }}

- follow-up work includes combining dim_csf_teachers_trained and
  dim_csf_workshop_attendance into one dimensional model which describes 
  workshop data and content (eg. dim_csf_workshops). any outlying data from 
  dim_csf_teachers_trained should be migrated to either dim_teachers or dim_csf_teacher_trainings

*/

-- here we go
with 
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
    where user_type = 'student'     -- still don't understand this logic 
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

csf_stages_for_completion as ( -- still don't understand these files (js)
    select * 
    from {{ ref('seed_csf_stages_for_completion') }}
),

csf_plugged_stage_counts as (
    select * 
    from {{ ref('seed_csf_plugged_stage_counts') }}
),

user_stages as (
    select *,
        row_number() over(partition by user_id, script_id order by stage_started_at asc) as stage_order
    from {{ ref('dim_user_stages') }}
),

csf_started as (
    select 
        user_stages.user_id as teacher_id, -- BOLD: I think have a student_id and teacher_id instead of user_id's is the best path forward here (will update upstream models in this commit as well)
        scripts.script_id,
        scripts.script_name,
        school_year.school_year,
        user_stages.started_at,
        user_stages.stage_order,
    from user_stages 
    left join scripts 
        on user_stages.script_id = scripts.script_id 
    left join school_years 
        on user_stages.stage_started_at 
            between school_years.started_at and school_years.ended_at
    left join csf.csf_stages_for_completion as csfc 
        on user_stages.script_id = csfc.script_id
        and csfc.stage_number = user_stages.stage_number
),

csf_completed as (
    select 
        user_id,
        csf_started.script_id,
        script_name,
        school_year,
        started_at,
        case when csf_started.stage_order = cpsc.plugged_stage_counts
            then started_at end as completed_at,
        row_number() over(partition by user_id, school_year, )
        -- completed_at,
        stage_order 
    from csf_started 
    left join csf_plugged_stage_counts as cpsc 
        on csf_started.script_id = cpsc.script_id 
    left join sections 
        on 
)