{# key point: though this model is driven by user_levels,
this model is a students/ mart therefore, should occur at
the level of the individual **user**...
the primary dimensions here are those relating to a user's level #}

{# maybe a related table to this could be a fct_user_levels to show
overall progress completion rates and such... #}
{{
    config(
        materialized='incremental',
        unique_key='user_level_id'
    )
}}

with
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
    
    {% if is_incremental() %}
    
    where created_at > (select max(created_at) from {{ this }} )

    {% endif %}
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        ul.user_level_id,
        ul.user_id,
        ul.created_at as user_level_created_at,
        ul.level_id,
        ul.level_source_id,
        cs.stage_id,
        ul.script_id,
        sy.school_year,
        cs.course_name_true                     as course_name
    from user_levels as ul 
     join course_structure as cs 
        on ul.level_id = cs.level_id
        and ul.script_id = cs.script_id
    join school_years as sy 
        on ul.created_at between sy.started_at and sy.ended_at
)

select * 
from combined
