/* 
1. Design:
    school_year
    student_id
    teacher_id
    *coteacher_id
    section_id

2. Definitions:
    this table provides mapping across these foreign keys, 
    serving as an intermediary (xref) model
*/

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

section_instructors as (
    select distinct 
        teacher_id,
        section_id,
        is_section_owner
    from {{ ref('stg_dashboard__section_instructors') }}
),

combined as (
    select 
        sy.school_year, 
        followers.student_id,
        seci.teacher_id,
        seci.section_id,
        seci.is_section_owner,
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
    left join section_instructors   
        on sections.section_id = section_instructors.section_id
        and sections.teacher_id = section_instructors.teacher_id
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
        is_section_owner,
        school_id
    from combined
    where row_num = 1 
)
select * 
from final 
