-- model: dim_sections
-- scope: 1 row per section, per school year 
-- updates:
-- Apr 3, 24: reverted to original model; added section_instructors (js)
-- jira: dataops-548

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
        is_section_owner,
        school_id,
        school_year,
        count(distinct student_id) as num_students_added
    from {{ ref('int_section_mapping') }}
    {{ dbt_utils.group_by(5) }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        sec.*,
        sy.school_year as school_year_created
    from sections as sec 
    join school_years as sy
        on sec.created_at 
            between sy.started_at and sy.ended_at
),

final as (
    select 
        -- section
        comb.section_id,
        comb.section_name,
        comb.school_year_created, {# section_created_year? #}
        
        comb.teacher_id,
        sm.is_section_owner,

        comb.login_type, 
        comb.grade,
        
        -- school 
        sm.school_year,
        sm.school_id,
        
        -- courses
        act.course_name,
        isnull(act.is_active, 0) as is_active,
        act.num_students_active,
        sm.num_students_added,
        
        comb.created_at,
        comb.updated_at
    
    from combined as comb 
    left join section_mapping as sm 
        on comb.section_id = sm.section_id
    left join active_sections as act
        on sm.section_id = act.section_id
        and sm.school_year = act.school_year )
    
select * 
from final