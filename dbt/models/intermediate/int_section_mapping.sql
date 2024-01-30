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

teacher_school_changes as (
    select *
    from {{ ref('int_teacher_schools_historical') }}
),

sections as (
    select distinct 
        teacher_id,
        section_id
    from {{ ref('stg_dashboard__sections') }}
),

combined as (
    select 
        sy.school_year, 
        followers.student_id,
        sections.teacher_id,
        sections.section_id         as section_id,
        tsc.school_id,
        row_number() over(
            partition by 
                followers.student_id, 
                sy.school_year
                order by 
                    followers.created_at
        ) as row_num

    from followers  
    left join sections 
        on followers.section_id = sections.section_id
    -- left join teachers 
    --     on sections.user_id = teachers.teacher_id
    join school_years as sy 
        on followers.created_at between sy.started_at and sy.ended_at
    left join teacher_school_changes tsc 
        on sections.teacher_id = tsc.teacher_id 
        and sy.ended_at between tsc.started_at and tsc.ended_at 
),

final as (
    select 
        student_id,
        school_year,
        section_id,
        teacher_id,
        school_id
    from combined
    where row_num = 1
)

select * 
from final 