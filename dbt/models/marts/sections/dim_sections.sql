with 
school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

teacher_school_changes as (
    select *
    from {{ ref('int_teacher_schools_historical') }}
),

sections as (
    select 
        section_id,
        section_name, 
        teacher_id,
        login_type,
        grade,
        created_at,
        updated_at 
    from {{ ref('stg_dashboard__sections') }}
),

int_section_mapping as (
    select * 
    from {{ ref('int_section_mapping') }}
),

section_metrics as (
    select
        section_id,
        teacher_id,
        school_id,
        school_year,
        count(distinct student_id) as num_students_added
    from int_section_mapping
    group by 1, 2, 3, 4
),

active_sections as (
    select 
        section_id,
        school_year,
        num_students,
        course_name,
        1 as is_active
    from {{ ref('int_active_sections') }}
),

all_sections_mapping as (
    select 
        sections.*, 
        tsc.school_id,
        sy.school_year as created_at_school_year
    from sections 
    join school_years sy 
        on sections.created_at between sy.started_at and sy.ended_at 
    join teacher_school_changes tsc 
        on sections.teacher_id = tsc.teacher_id 
        and sy.ended_at between tsc.started_at and tsc.ended_at 
)

select 
    asm.section_id,
    asm.teacher_id,
    asm.school_id,
    asm.created_at_school_year,
    section_metrics.school_year                                 as added_students_school_year,
    asm.section_name, 
    asm.login_type,
    asm.grade,
    asm.created_at,
    asm.updated_at,
    section_metrics.num_students_added,
    active_sections.num_students                                as num_students_active,
    active_sections.course_name,
    case when active_sections.is_active = 1 then 1 else 0 end   as is_active 
from all_sections_mapping asm
left join section_metrics 
    on section_metrics.section_id = asm.section_id
left join active_sections
    on section_metrics.section_id = active_sections.section_id
    and section_metrics.school_year = active_sections.school_year