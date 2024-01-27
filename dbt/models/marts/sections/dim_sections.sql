with 
sections as (
    select 
        section_id,
        section_name, 
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
    from {{ ref('int_section_mapping') }}
    group by 1, 2, 3, 4
),

active_sections as (
    select 
        section_id,
        school_year,
        num_students,
        1 as is_active
    from {{ ref('int_active_sections') }}
)

select 
    section_metrics.section_id,
    section_metrics.teacher_id,
    section_metrics.school_id,
    section_metrics.school_year,
    sections.section_name, 
    sections.login_type,
    sections.grade,
    sections.created_at,
    sections.updated_at,
    section_metrics.num_students_added,
    active_sections.num_students                                as num_students_active,
    case when active_sections.is_active = 1 then 1 else 0 end   as is_active 
from section_metrics
left join sections 
    on section_metrics.section_id = sections.section_id
left join active_sections
    on section_metrics.section_id = active_sections.section_id
    and section_metrics.school_year = active_sections.school_year

