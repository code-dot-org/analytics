/* 
1. Design:
    school_year
    student_id
    teacher_id
    section_id
    school_id
    school_info_id

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
    -- this needs to be dimensionalized
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
        sections.section_id                                                 as section_id,
        tsc.school_id,
        tsc.school_info_id,
        student_added_at,
        case 
            when 
                followers.student_removed_at is null 
                or followers.student_removed_at > sy.ended_at 
            then sy.ended_at
            else followers.student_removed_at
        end                                                                 as student_removed_at,
        row_number() over(
            partition by 
                followers.student_id, 
                sy.school_year,
                sections.section_id
                order by 
                    followers.student_added_at
        ) as row_num

    from followers  
    left join sections 
        on followers.section_id = sections.section_id
    join school_years as sy 
        on followers.student_added_at between sy.started_at and sy.ended_at
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
        school_id,
        school_info_id,
        student_added_at,
        student_removed_at
    from combined
    where row_num = 1 
)
select * 
from final 
