{#
model: dim_course_structure (v2)
auth: nzm
notes:
changelog:
    - dataops-668: 
        updated to reflect updates to analysis.course_structure by NZM
    - dataops-...
        updated to add distinct clause to final table*

#}

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

contained_levels as (
    select * 
    from {{ ref("stg_dashboard__contained_levels") }}
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
    select distinct 
    
        -- courses
        ug.unit_group_id    as course_id,
        ug.unit_group_name  as course_name,
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
        st.is_lockable,
        st.is_unplugged,

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
        case 
            when plcl.parent_level_id is not null 
            then 1 else 0 end       as is_parent_level,

        -- contained levels 
        col.level_group_level_id,
        case 
            when col.level_group_level_id is not null 
            then 1 else 0 
        end                         as is_group_level,
        col.contained_level_type    as group_level_type,

        coalesce(
            ug.family_name,
            sc.family_name)     as family_name, 

        coalesce(
            ug.version_year, 
            sc.version_year)    as version_year,

        coalesce(
            ug.published_state,
            sc.published_state) as published_state,

        coalesce(
            ug.instruction_type,
            sc.instruction_type)    as instruction_type,
        
        coalesce(
            ug.instructor_audience,
            sc.instructor_audience) as instructor_audience,
        
        coalesce(
            ug.participant_audience,
            sc.participant_audience)    as participant_audience,
        
        lev.updated_at                  as updated_at

    from scripts as sc 

    left join script_levels as sl 
        on sc.script_id = sl.script_id
    
    left join levels_script_levels as lsl 
        on lsl.script_level_id = sl.script_level_id
    
    left join stages as st 
        on st.stage_id = sl.stage_id
    
    left join levels as lev
        on lev.level_id = lsl.level_id 
    
    left join course_scripts as cs 
        on sc.script_id = cs.script_id
    
    left join unit_groups as ug 
        on ug.unit_group_id = cs.course_id 
    
    left join course_names as cn 
        on ug.unit_group_id = cn.versioned_course_id   
    
    left join script_names as sn 
        on sn.versioned_script_id = sc.script_id
    
    left join parent_levels_child_levels as plcl 
        on plcl.parent_level_id = lsl.level_id 
    
    left join contained_levels as col 
        on lsl.level_id = col.level_group_level_id )

select *
from combined 



{#
    notes [js,JUN03]
    
    current                 442,473
    distinct                59,250 *
    remove parent_levels    387,204

#} 

