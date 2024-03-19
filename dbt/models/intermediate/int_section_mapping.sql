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

sections as (
    select * 
    from {{ ref('stg_dashboard__sections')}}
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
        foll.student_id,
        sec.teacher_id,
        sec.section_id,
        sei.is_section_owner,
        tsc.school_id,
        
        row_number() over(
            partition by 
                foll.student_id, 
                sy.school_year
                order by 
                    foll.created_at
        ) as row_num

    from followers as foll 
    left join sections as sec 
        on foll.section_id = sec.section_id
    left join section_instructors as sei 
        on sec.section_id = sei.section_id
        and sec.teacher_id = sei.teacher_id
    join school_years as sy 
        on foll.created_at 
            between sy.started_at 
                and sy.ended_at
    left join teacher_school_changes as tsc 
        on sec.teacher_id = tsc.teacher_id 
        and sy.ended_at 
            between tsc.started_at 
            and tsc.ended_at 
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
