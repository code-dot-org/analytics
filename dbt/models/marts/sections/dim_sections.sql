{# 

Re-write of dim_sections by Baker on 2.6.24

Table creates one record per section/teacher per school year REGARDLESS of activity in the section.
Sections that were created but never active have NULL activity.

I kept all columns and names from original dim_sections, but I think we might consider modifying 
some of them for clarity -- noted in inline comments below.

This model should have every distinct section_id that appears in stg__sections.  However, some sections 
will show students added but no activity because the activity is excluded by int_active_sections.

#}

with school_years as (
    select * 
    from {{ ref('int_school_years') }}
),
teacher_school_changes as (
    select *
    from {{ ref('int_teacher_schools_historical') }}
)
, all_sections as (
    select 
        section_id,
        section_name, 
        teacher_id,
        login_type,
        grade,
        created_at,
        sy.school_year as school_year_created,
        updated_at 
    from {{ ref('stg_dashboard__sections') }} as sec
    inner join
        school_years as sy
        on sec.created_at between sy.started_at and sy.ended_at
)
, num_students_per_section as (
    select 
        section_id, 
        teacher_id,
        school_id,
        school_year,
        count(distinct student_id) as num_students_added
    
    from {{ ref('int_section_mapping') }}
    {{ dbt_utils.group_by(4) }}
)
, teacher_active_courses as (
    select 
        teacher_id,
        section_id,
        school_year,
        course_name,
        section_started_at,
        1 as is_active,
        num_students as num_students_active
    from {{ ref('int_active_sections') }}
)
,teacher_active_courses_with_sy as (

    select
        tac.teacher_id,
        tac.section_id,
        tac.school_year,
        tac.course_name,
        tac.section_started_at,
        tac.is_active,
        tac.num_students_active,
        tsc.school_id
    from teacher_active_courses as tac 
    inner join school_years as sy
        on tac.school_year = sy.school_year
    left join teacher_school_changes as tsc 
        on
            tac.teacher_id = tsc.teacher_id 
            and sy.ended_at between tsc.started_at and tsc.ended_at 
)
, final as (
    select

        -- general section data from the sections table
        sec.section_id,
        sec.teacher_id,
        sec.school_year_created,
        sec.section_name,
        sec.login_type,
        sec.grade,
        sec.created_at, 
        sec.updated_at,

        -- data from followers based on school year (number of students added to the section within the active school year)
        nsps.num_students_added,

        -- section activity within a school/school year
        act.school_id,
        act.course_name,
        act.is_active,
        act.num_students_active,
        act.section_started_at,     
        coalesce(act.school_year, nsps.school_year) school_year    -- coalesce first activity school_year with year of student activity
                                                                
    from all_sections as sec

    left join num_students_per_section as nsps 
        on
            sec.section_id = nsps.section_id
            and sec.teacher_id = nsps.teacher_id

    left join teacher_active_courses_with_sy as act
        on
            nsps.section_id = act.section_id
            and nsps.school_year = act.school_year
            and nsps.teacher_id = act.teacher_id
            and nsps.school_id = act.school_id 
)
select *
from final