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

-- select all courses that should be categorized as active student courses, to account for those with null participant audience
student_course_names as (
    select distinct course_name
    from {{ ref('stg_dashboard__scripts') }}
    where participant_audience = 'student'
    and course_name not like '%self paced pl%'
    and course_name not in ('hoc', 'other')
),

script_names as (
    select * 
    from {{ ref('dim_script_names') }} 
),

combined as (
    select distinct 
        -- courses
        ug.unit_group_id                                                as course_id,
        ug.unit_group_name                                              as course_name_full,
        sc.course_name,

        --flags
        case 
            when 
                coalesce(
                    ug.instruction_type,
                    sc.instruction_type
                ) = 'self_paced' 
            then 1 
            else 0 
        end                                                             as is_self_paced,    
        case 
            when 
                coalesce(
                    ug.participant_audience,
                    sc.participant_audience
                )  = 'student' 
            then 1 
            else 0 
        end                                                             as is_student_content,
        case 
            when 
                coalesce(
                    ug.participant_audience,
                    sc.participant_audience
                )  = 'teacher' 
            then 1 
            else 0 
        end                                                             as is_pd_content,

        case 
            when 
                (
                    coalesce(
                        ug.participant_audience,
                        sc.participant_audience
                    )   = 'student'
                    or coalesce(
                        ug.participant_audience,
                        sc.participant_audience
                    )   is null
                )
                and sc.course_name in (
                    select course_name from student_course_names
                )                                                             
            then 1 
            else 0 
        end                                                             as is_active_student_course,

        -- surrogate key for level_script_id
        {{ dbt_utils.generate_surrogate_key(
            ['lev.level_id', 
             'sc.script_id']) }}                                        as level_script_id,


        -- scripts
        sl.script_id,
        sc.script_name,
        sc.is_standalone,
        sc.unit,
        
        -- stages
        st.stage_id,
        st.stage_name,
        st.absolute_position                                            as stage_number,
        st.relative_position,
        st.is_lockable,
        st.is_unplugged,

        -- script_levels
        sl.is_assessment,
        sl.is_challenge,
        sl.position                                                     as level_number,

        -- levels
        -- custom calc for level_id
        case when sl.script_id = '26' 
              and lsl.level_id = '14633' 
             then 1 else lsl.level_id 
        end                                                             as level_id,

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
            then 1 else 0 end                                           as is_parent_level,

        -- contained levels 
        col.level_group_level_id,
        case 
            when col.level_group_level_id is not null 
            then 1 else 0 
        end                                                             as is_group_level,
        col.contained_level_type                                        as group_level_type,

        coalesce(
            ug.family_name,
            sc.family_name)                                             as family_name, 

        coalesce(
            ug.version_year, 
            sc.version_year)                                            as version_year,

        coalesce(
            ug.published_state,
            sc.published_state)                                         as published_state,

        coalesce(
            ug.instruction_type,
            sc.instruction_type)                                        as instruction_type,
        
        coalesce(
            ug.instructor_audience,
            sc.instructor_audience)                                     as instructor_audience,
        
        coalesce(
            ug.participant_audience,
            sc.participant_audience)                                    as participant_audience,
        
        lev.updated_at                                                  as updated_at

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
    
    left join parent_levels_child_levels as plcl 
        on plcl.parent_level_id = lsl.level_id 
    
    left join contained_levels as col 
        on lsl.level_id = col.level_group_level_id
),

final as (
    select 
        course_id,
        course_name_full,	
        course_name,
        is_self_paced,	
        is_student_content,
        is_pd_content,
        is_active_student_course,	
        script_id,	
        script_name,	
        is_standalone,	
        unit,
        stage_id,
        stage_name,
        stage_number,
        relative_position,	
        is_lockable,
        is_unplugged,	
        is_assessment,	
        is_challenge,	
        level_number,	
        level_id,
        level_script_id,
        dense_rank() over(
                partition by script_id 
                order by 
                    stage_number, 
                    level_number) as level_script_order,
        level_name,
        level_type,	
        mini_rubric,
        is_free_play,
        project_template_level_name,	
        is_submittable,
        parent_level_kind,	
        is_parent_level,	
        level_group_level_id,	
        is_group_level,
        group_level_type,	
        family_name,
        version_year,	
        published_state,	
        instruction_type,
        instructor_audience,	
        participant_audience,	
        updated_at              
    from combined where script_id is not null) -- excludes empty scripts

select *
from final