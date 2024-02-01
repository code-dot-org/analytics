with 
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

active_sections as (
    select 
        section_id,
        school_year,
        course_name,
        1 as is_active,
        num_students as num_students_active
    from {{ ref('int_active_sections') }}
),

section_mapping as (
    select 
        section_id, 
        teacher_id,
        school_id,
        school_year,
        count(distinct student_id) as num_students_added
    from {{ ref('int_section_mapping') }}
    {{ dbt_utils.group_by(4) }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

teacher_school_changes as (
    select 
        school_id,
        teacher_id,
        started_at,
        ended_at
    from {{ ref('int_teacher_schools_historical') }}
),

combined as (
    select 
        sec.*,
        tsc.school_id,
        sy.school_year as school_year_created
    from sections as sec 
    join school_years as sy
        on sec.created_at 
            between sy.started_at and sy.ended_at
    join teacher_school_changes as tsc 
        on sec.teacher_id = tsc.teacher_id
        and sy.ended_at 
            between tsc.started_at and tsc.ended_at
),

final as (
    select 
        comb.section_id,
        comb.teacher_id,
        comb.school_id,
        comb.school_year_created,
        comb.section_name,
        comb.login_type, 
        comb.grade,
        comb.created_at,
        comb.updated_at,
        
        act.course_name,
        isnull(act.is_active, 0) as is_active,
        act.num_students_active,
    
        sm.num_students_added,
        sm.school_year as school_year
    from combined as comb 
    left join section_mapping as sm 
        on comb.section_id = sm.section_id
    left join active_sections as act
        on sm.section_id = act.section_id
        and sm.school_year = act.school_year
)
    
select * 
from final 