-- model: dim_curriculum_survey_responses.sql 

with 
student_surveys as (
    select * 
    from {{ ref('dim_curriculum_surveys') }}
),

level_sources as (
    select * 
    from {{ ref('stg_dashboard_pii__level_sources') }}
),

user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels' )}}
),

users as (
    select * 
    from {{ ref('dim_users') }}
),

free_responses as (
    select * 
    from {{ ref('stg_dashboard__level_sources_free_responses') }}
),

answer_texts as (
    select *
    from {{ ref('stg_dashboard__contained_level_answers') }}
),

school_years as (
    select * 
    from {{ref('int_school_years')}}
),

combined as (
    select 
        ul.user_id as student_id,
        ul.created_at,
        sy.school_year,
        usr.country,
        usr.us_intl,
        usr.gender,
        usr.races,
        usr.is_urg,

        ss.group_level_id,
        ss.content_area,
        ss.course_name,
        ss.script_id,
        ss.script_name,
        ss.unit, 
        ss.topic_tags,
        ss.version_year,
        ss.survey_level_id,
        ss.survey_name,
        ss.survey_type,
        ss.contained_level_id,
        ss.question_name,
        ss.question_type,
        ss.question_number,
        ss.question_text,
        
        ant.answer_number,
        case 
            when ss.question_type = 'multi' 
            then ant.answer_text
            
            when ss.question_type = 'freeresponse'
            then coalesce(lsfr.data,ls.data) 
            
        end as answer_response -- answer chosen or written by student 

    from student_surveys    as ss 

    join user_levels   as ul 
     on ul.script_id    = ss.script_id 
    and ul.level_id     = ss.contained_level_id 

    left join level_sources as ls 
     on ul.level_source_id = ls.level_source_id
    
    left join answer_texts as ant
     on ls.level_id = ant.level_id 
    and ls.data = ant.answer_number 

    left join free_responses as lsfr 
    on ul.level_source_id = lsfr.level_sources_free_response_id

    join users              as usr 
    on ul.user_id = usr.user_id 

    join school_years       as sy 
    on ul.created_at 
        between sy.started_at 
            and sy.ended_at )

select * 
from combined 