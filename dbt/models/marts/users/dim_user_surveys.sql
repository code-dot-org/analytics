model: dim_user_surveys
scope: tbd
source: public.nzm_view_user_surveys

with 
parent_child_levels as (
    select 
        parent_levels_id,
        child_level_id
    from {{ ref('int_parent_levels_child_levels') }}
),

levels as (
    select * 
    from {{ ref('dim_levels') }}
),

user_levels as (
    select * 
    from {{ ref('dim_user_levels') }}
),

users as (
    select * 
    from {{ ref('dim_users') }}
    where country = 'united states'
        -- and user_id in (select user_id from user_levels)
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
    from di
)