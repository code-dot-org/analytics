/* 
1. Design:
    school_year
    student_id
    teacher_id
    section_id

2. Definitions:
    this table provides mapping across these foreign keys, 
    serving as an intermediary (xref) model

3. Sources:

Ref: DATAOPS-321 */

with 
school_years as (
    select *
    from {{ ref('int_school_years') }}
),

followers as (
    select *
    from {{ ref('stg_dashboard__followers') }}
),

teachers as (
    select distinct 
        teacher_id,
        school_id
    from {{ ref('dim_teachers') }}
),

sections as (
    select distinct 
        user_id,
        section_id
    from {{ ref('dim_sections') }}
),

combined as (
    select 
        school_years.school_year, 
        followers.student_id,
        sections.user_id            as teacher_id,
        sections.section_id         as section_id,
        teachers.school_id
    from followers  
    left join sections 
        on followers.section_id = sections.section_id
    left join teachers 
        on sections.user_id = teachers.teacher_id
    join school_years 
        on followers.created_at 
            between school_years.started_at and school_years.ended_at
)

select *
from combined