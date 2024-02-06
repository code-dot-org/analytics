{# 
Re-write of dim_sections by Baker on 2.6.24

Table creates one record per section/teacher per school year REGARDLESS of activity in the section.
Sections that were created but never active have NULL activity.

I kept all columns and names from original dim_sections

Because this table is derived from int_section_mapping, the number of distinct sections will not
match the total number of sections (some get excluded through int_section_mapping)
but it should match, and contain, the same sections that are in int_section_mapping + 
all sections that were never activited or had any followers.

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
-- should we attach followers here to show sections that have followers but don't get mapped?
, num_students_per_section as (
    select 
        section_id, 
        teacher_id,
        school_id,
        school_year,
        count(distinct student_id) as num_students_added
    
    from {{ ref('int_section_mapping') }}               --this table limits each student to 1 section per year
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
        --
        tac.section_id,
        --
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

        -- general section stuff
        sec.section_id,
        sec.teacher_id,
        act.school_id,
        sec.school_year_created,
        sec.section_name,
        sec.login_type,
        sec.grade,
        sec.created_at, 
        sec.updated_at,

        -- section activity within school year
        act.course_name,

        act.is_active,  -- I think is_active is potentially confusing/misleading here because of the school_year as part of the grain.  
                        -- It means WAS active in the the school year for which this section was active.
                        -- There are also other clues in the fields that this was active in a particular year

        act.num_students_active,
        nsps.num_students_added,
        --students_added_school_year,  --was failing a test where 6 columns where the same except for num_students_added.  This means that the section added students in different school year, but had no official acitivity.  Also means there are 3 SYs we need to consider.  SY created _at, SY students added, SY activity_started_at

        act.section_started_at, --adding response to request
        coalesce(act.school_year, nsps.school_year) school_year-- coalesce because some sections have students added in a school year but no measureable activity.  These school_years are the same in cases were both exist. Q: should this be called active_school_year or something like that?

        
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
            and nsps.school_id = act.school_id --it's possible we don't want to include this because of weird null stuff
)
select *
from final
