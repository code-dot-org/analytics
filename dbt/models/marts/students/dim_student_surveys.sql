-- model: dim_student_surveys 

with 
contained_levels as (
    select * 
    from {{ ref('stg_dashboard__contained_levels') }}
),

contained_levels_answers as (
    select distinct 
        level_id, answer_number, answer_text
    from {{ ref('stg_dashboard__contained_level_answers') }}
),

levels as (
    select 
        *,   
        case 
            when level_type = 'multi' then json_array_length(json_extract_path_text(properties, 'answers'))
            when level_type = 'freeresponse' 	then 1 
            when level_type = 'external'		then 0 
        end 	as num_response_options 

    from {{ ref('stg_dashboard__levels') }}
),

script_levels as (
    select *
    from {{ ref('stg_dashboard__script_levels') }}
),

levels_script_levels as (
    select * 
    from {{ ref('stg_dashboard__levels_script_levels') }}
),

scripts as (
    select * 
    from {{ ref('stg_dashboard__scripts') }}   
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
),

combined as (
    select 
        gl.level_id                     as group_level_id,
        lower(gl.level_name)            as survey_name,
        
        case 
            when survey_name like '%pre%' 
             and survey_name not like '%preview%'  then 'pre'
            when survey_name like '%post%'         then 'post'
            when survey_name like '%pulse%'        then 'pulse'
            when survey_name like '%end of unit%'  then 'end of unit'
            else null end              as survey_type,
        
        sc.script_name,
        cl.level_id                     as contained_level_id,
        cl.level_name                   as question_name,
        cl.level_type                   as question_type,
        col.contained_level_position    as question_position,
        col.contained_level_text        as question_text,
        
        cl.num_response_options,
        cola.answer_text        as response_option,
        cola.answer_number,    
        
        cs.level_script_id,
        cs.course_name,
        cs.unit, 
        cs.content_area,
        cs.topic_tags,
        cs.version_year

    from contained_levels   as col 
    join levels             as gl -- group levels 
    on col.level_group_level_id = gl.level_id

    join levels             as cl -- contained levels 
    on col.contained_level_id = cl.level_id

    left join contained_levels_answers as cola 
    on cl.level_id = cola.level_id  
    
    join levels_script_levels   as lsl 
    on gl.level_id = lsl.level_id
    
    join script_levels          as sl 
    on lsl.script_level_id = sl.script_level_id 

    join scripts as sc 
    on sl.script_id = sc.script_id 

    join course_structure as cs 
    on sc.script_id = cs.script_id 
    and gl.level_id = cs.level_id 

    where gl.level_name like '%survey%' )

select *
from combined