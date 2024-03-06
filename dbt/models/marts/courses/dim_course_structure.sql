{# needs optimization #}

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
        c.course_id,
        cn.course_name_short,
        cn.course_name_long,

        -- scripts
        sl.script_id,
        sc.script_name,
        sn.versioned_script_name,
        sn.script_name_short,
        sn.script_name_long,
        
        -- stages
        st.stage_id,
        st.stage_name,
        st.relative_position,
        st.absolute_position as stage_number
        st.lockable as is_lockable,

        -- levels
        case when sl.script_id = '26' 
              and lsl.level_id = '14633' 
             then 1 else lsl.level_id 
        end as level_id,
        lev.name as level_name,
        sl.position as level_number,
        case when sl.assessment = 1 then 1 else 0 end as is_assessment,



        rank() over(partition by sl.script_id order by st.stage_number, sl.position) as level_script_order,
        sl.is_assessment,
        lev.updated_at
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
        on sc.script_id = sn.versioned_script_id
)

select * 
from combined;


/***********************************************
  -- nzm 2024-02-21
    select
        distinct cn.course_name_short -- manually updated (no longer updating)
,
        cn.course_name_long -- manually updated (no longer updating)
,
        sn.script_name_short -- manually updated (no longer updating)
,
        sn.script_name_long -- manually updated (no longer updating)
,
        c.id course_id,
        c.name course_name,
        sc.id script_id,
        sn.versioned_script_name -- manually updated (no longer updating)
,
        sc.name script_name -- /* stage (lesson) info */
,
        st.id stage_id,
        st.name stage_name,
        --      case
        --        when lockable = 1 then st.absolute_position
        --        else
        st.relative_position --      end
,
        st.absolute_position stage_number -- removed the case statement because it led to multiple units with the same position unnecessarily
,
        st.lockable lesson_lockable,
        json_extract_path_text(st.properties, 'unplugged', true) lesson_unplugged -- /* level info */
,
        case
        when sl.script_id = '26'
        and lsl.level_id = '14633' then '1'
        else lsl.level_id end as level_id --      --------------------- hard coded error correction, level_script_levels defines the first level of this script as id# 14,633 when user_levels defines this level as #1
,
        le.name level_name,
        sl.position as level_number,
        case
        when sl.assessment = 1 then 1
        else 0 end as assessment,
        case
        when script_name like 'devices-20__' then 'csd'
        when script_name like '%hello%' then 'hoc'
        when script_name like 'microbit%' then 'csd'
        when script_name like 'csd-post-survey-20__' then 'csd'
        when script_name like 'csp-post-survey-20__' then 'csp'
        when json_extract_path_text(sc.properties, 'curriculum_umbrella', true) = '' then 'other'
        else lower(
            json_extract_path_text(sc.properties, 'curriculum_umbrella', true)
        ) end as course_name_true,
        rank () over (
            partition by sl.script_id
            order by
                stage_number,
                sl.position
        ) level_script_order,
        le.updated_at as updated_at --      natalia's version
        /* course and script info, script inherits course info if it belongs to a course */
,
        coalesce(
            json_extract_path_text(c.properties, 'family_name', true) -- from course info if available
,
            sc.family_name
        ) family_name -- from script if course info not available
,
        json_extract_path_text(sc.properties, 'is_course', true) is_standalone,
        regexp_replace (sc.name, '((-)+\\d{4})', '') unit,
        coalesce(
            json_extract_path_text(c.properties, 'version_year', true) -- from course info if available
,
            json_extract_path_text(sc.properties, 'version_year', true) -- from script if course info not available
        ) version_year,
        coalesce(c.published_state, sc.published_state) published_state -- from course info if available, from script if not
,
        coalesce(c.instruction_type, sc.instruction_type) instruction_type -- from course info if available, from script if not
,
        coalesce(c.instructor_audience, sc.instructor_audience) instructor_audience -- from course info if available, from script if not
,
        coalesce(c.participant_audience, sc.participant_audience) participant_audience -- from course info if available, from script if not
        /* level info */
,
        le.type level_type,
        case
        when json_extract_path_text(sl.properties, 'challenge', true) = 'true' then 1
        else 0 end as challenge,
        case
        when json_extract_path_text(le.properties, 'mini_rubric', true) = 'true' then 1
        else 0 end as mini_rubric,
        case
        when json_extract_path_text(le.properties, 'free_play', true) = 'true' then 1
        else 0 end as free_play,
        json_extract_path_text(le.properties, 'project_template_level_name', true) project_template_level_name,
        json_extract_path_text(le.properties, 'submittable', true) submittable
    from
        dashboard_production.scripts sc -- starting off from scripts allows to include scripts without levels (e.g. ai-ethics)

        left join dashboard_production.script_levels sl on sc.id = sl.script_id
        
        left join dashboard_production.levels_script_levels lsl on sl.id = lsl.script_level_id
        
        left join dashboard_production.stages st on st.id = sl.stage_id
        
        left join dashboard_production.levels le on le.id = lsl.level_id
        
        left join dashboard_production.course_scripts cs on cs.script_id = sc.id
        
        left join dashboard_production.unit_groups c on c.id = cs.course_id
        
        left join analysis.course_names cn on cn.versioned_course_id = c.id -- manually updated spreadsheet
        
        left join analysis.script_names sn on sn.versioned_script_id = sc.id -- manually updated spreadsheet
        
***********************************************/