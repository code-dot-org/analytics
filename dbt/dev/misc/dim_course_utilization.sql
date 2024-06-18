-- Course Utilization Dashboard 
-- js; 2024-05-02

with 
course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
),

scripts as (
    select * 
    from {{ ref('stg_dashboard__scripts') }}
),

user_leves as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
),

levels as (
    select * 
    from {{ ref('stg_dashboard__levels') }}
),

stages as (
    select * 
    from {{ ref('stg_dashboard__stages') }}
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
)