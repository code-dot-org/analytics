{#
    model: dim_user_surveys
    scope: tbd
    source: public.nzm_view_user_surveys
#}

with 
parent_levels_child_levels as (
    select 
        parent_level_id,
        child_level_id
    from {{ ref('int_parent_levels_child_levels') }}
),

levels as (
    select * 
    from {{ ref('dim_levels') }}
    where level_name like '%survey%'
),

parent_levels as (
    select * 
    from levels 
    where level_id in (
        select parent_level_id 
        from parent_level_child_levels)
),

child_levels as (
    select * 
    from levels 
    where level_id in (
        select child_level_id 
        from parent_levels_child_levels)
),

levels_script_levels as (
    select * 
    from {{ ref('stg_dashboard__levels_script_levels') }}
    where level_id in (
        select parent_level_id 
        from parent_levels_child_levels)
),

user_levels as (
    select * 
    from {{ ref('dim_user_levels') }}
),

users as (
    select * 
    from {{ ref('dim_users') }}
    where country = 'united states'
),

sections as (
    select * 
    from {{ ref('dim_sections') }}
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
    where course_name_true in ('csp','csd','csa')
),

scripts as (
    select * 
    from {{ ref('stg_dashboard__scripts') }}
    where script_name not in (
        'allthesurveys',
        'allthethings',
        'csp-pre-survey-test-2017')
),

