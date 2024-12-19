-- model: dim_student_surveys 

with 
course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
        where content_area <> 'other'
        and level_name like '%survey%'
)

, contained_levels as (
    select *,
        dense_rank() over(
            partition by 
                level_group_level_id
            order by 
                contained_level_page, 
                contained_level_position asc) as question_number
    from {{ ref('stg_dashboard__contained_levels') }}
),

contained_levels_answers as (
    select distinct 
        level_id, answer_number, answer_text
    from {{ ref('stg_dashboard__contained_level_answers') }}
)

, levels as (
    select 
        *,   
        case 
            when level_type = 'multi' then json_array_length(json_extract_path_text(properties, 'answers'))
            when level_type = 'freeresponse' 	then 1 
            when level_type = 'external'		then 0 
        end 	as num_response_options 
    from {{ ref('stg_dashboard__levels') }}
)

, final as (
   select 
  		cs.level_id                     as group_level_id,
        cs.content_area,
        cs.course_name,
        cs.script_id,
        cs.script_name,
        cs.unit, 
        cs.topic_tags,
        cs.version_year,
                col.level_group_level_id        as survey_level_id,
        lower(cs.level_name)            as survey_name,
        cs.level_script_id,

        case 
            when survey_name like '%pre%' 
             and survey_name not like '%preview%'  then 'pre'
            when survey_name like '%post%'         then 'post'
            when survey_name like '%pulse%'        then 'pulse'
            when survey_name like '%end of unit%'  then 'end of unit'
            else null end              as survey_type,

        col.contained_level_id,
        cl.level_name                   as question_name,
        cl.level_type                   as question_type,
        col.question_number,
        col.contained_level_text        as question_text,

        cl.num_response_options,
        cola.answer_text,       
        cola.answer_number
        from course_structure cs
        
        join contained_levels   as col 
    	on cs.level_id  = col.level_group_level_id
    	
    	join levels             as cl -- contained levels 
    	on col.contained_level_id = cl.level_id
    	
    	left join contained_levels_answers as cola 
    	on cl.level_id = cola.level_id  
)

select * 
from final 
