/* 
1. Design:
    school_year
    section_id
    status
    -- student_id
    -- teacher_id
    -- school_id

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
    select distinct 
        student_id,
        section_id,
        created_at
    from {{ ref('stg_dashboard__followers') }}
),

teachers as (
    select distinct
        teacher_id
    from {{ ref('stg_dashboard__users') }}
),

sections as (
    select distinct 
        user_id,
        section_id
    from {{ ref('stg_dashboard__sections') }}
),

combined as (
    select 
        school_years.school_year, 
        followers.student_id,
        sections.section_id         as section_id,
        sections.user_id            as teacher_id
    from followers  
    left join sections 
        on followers.section_id = sections.section_id
    left join teachers 
        on sections.user_id = teachers.teacher_id
    join school_years 
        on followers.created_at 
            between school_years.started_at and school_years.ended_at
),

section_status as (
    select 
        school_year, 
        section_id,
        count(distinct student_id) as total_students
    from combined
    {{ dbt_utils.group_by(2) }}
    having total_students >= 5
),

final as (
    select 
        combined.school_year,
        combined.section_id,
        combined.teacher_id,
        case when section_status.section_id then 1 else 0 end as is_active
    from combined
    left join section_status
        on combined.section_id = section_status.section_id 
        and combined.school_year = section_status.school_year
)

select * 
from final 