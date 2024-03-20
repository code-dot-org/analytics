{#-
model: dim_course_structure (v2)
auth: js, 2024-03-07
notes:
    - updated to reflect updates to analysis.course_structure by NZM
-#}

with 
scripts as (
    select *
    from {{ ref('stg_dashboard__scripts') }} 
),

levels as (
    select * 
    from {{ ref('stg_dashboard__levels') }}
    ),

stages as (
    select *
    from {{ ref('stg_dashboard__stages') }}
),

script_levels as (
    select *
    from {{ ref('stg_dashboard__script_levels') }}
),

levels_script_levels as (
    select * 
    from {{ ref('stg_dashboard__levels_script_levels') }}
),

course_scripts as (
    select * 
    from {{ ref('stg_dashboard__course_scripts') }}
),

unit_groups as (
    select * 
    from {{ ref('stg_dashboard__unit_groups') }}
),

parent_levels_child_levels as (
    select *
    from {{ ref('int_parent_levels_child_levels') }}
),

course_names as (
    select *
    from {{ ref('dim_course_names') }}
),

script_names as (
    select * 
    from {{ ref('dim_script_names') }} 
),

combined as (
    select 
        -- courses
        ug.unit_group_id as course_id,
        cn.course_name_short,
        cn.course_name_long,
        sc.course_name_true,

        -- scripts
        sl.script_id,
        sc.script_name,
        sn.versioned_script_name,
        sn.script_name_short,
        sn.script_name_long,
        sc.is_standalone,
        sc.unit,
        
        -- stages
        st.stage_id,
        st.stage_name,
        st.absolute_position as stage_number,
        st.relative_position,
        st.is_lesson_lockable,

        -- script_levels
        sl.is_assessment,
        sl.is_challenge,
        sl.position as level_number,

        -- levels
        -- custom calc for level_id
        case when sl.script_id = '26' 
              and lsl.level_id = '14633' 
             then 1 else lsl.level_id 
        end as level_id,

        rank() over(
            partition by sl.script_id 
            order by 
                st.stage_number, 
                sl.position)    as level_script_order,

        lev.level_name,
        lev.level_type,
        lev.mini_rubric,
        lev.is_free_play,
        lev.project_template_level_name,
        lev.is_submittable,

        -- parent_child_levels
        plcl.parent_level_kind,
        case when plcl.parent_level_id is not null 
            then 1 
                else 0 
        end                     as is_parent_level,

        coalesce(
            ug.family_name,
            sc.family_name)     as family_name, 
        -- from script if course info not available

        coalesce(
            ug.version_year, 
            sc.version_year)    as version_year,
        -- from script if course info not available

        coalesce(
            ug.published_state,
            sc.published_state) as published_state,
        -- from course info if available, from script if not

        coalesce(
            ug.instruction_type,
            sc.instruction_type)    as instruction_type,
        -- from course info if available, from script if not
        
        coalesce(
            ug.instructor_audience,
            sc.instructor_audience) as instructor_audience,
        -- from course info if available, from script if not
        
        coalesce(
            ug.participant_audience,
            sc.participant_audience)    as participant_audience,
        -- from course info if available, from script if not
        
        lev.updated_at                  as updated_at

    from levels_script_levels as lsl 
    join script_levels as sl 
        on lsl.script_level_id = sl.script_level_id
    join levels as lev
        on lsl.level_id = lev.level_id  
    join scripts as sc 
        on sl.script_id = sc.script_id
    join stages as st 
        on sl.stage_id = st.stage_id 
    left join course_scripts as cs 
        on sc.script_id = cs.script_id
    left join unit_groups as ug 
        on cs.course_id = ug.unit_group_id
    left join course_names as cn 
        on ug.unit_group_id = cn.versioned_course_id   
    left join script_names as sn 
        on sc.script_id = sn.versioned_script_id)
    left join parent_levels_child_levels as plcl 
        on lsl.level_id = plcl.child_level_id

select * 
from combined