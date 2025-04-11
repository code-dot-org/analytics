-- model: dim_curriculum_survey_responses.sql 

with 
student_surveys as (
    select distinct 
        group_level_id,
        content_area,
        course_name,
        script_id,
        script_name,
        unit, 
        topic_tags,
        version_year,
        survey_level_id,
        survey_name,
        survey_type,
        contained_level_id,
        question_name,
        question_type,
        question_number,
        question_text,
        num_response_options
    from {{ ref('dim_curriculum_surveys') }}
),

user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels' )}}
    where level_id in (select contained_level_id from student_surveys)
),

level_sources as (
    select * 
    from {{ ref('stg_dashboard_pii__level_sources') }}
    where level_source_id in (select level_source_id from user_levels)
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
    select distinct 
        level_id,
        answer_number,
        answer_text
    from {{ ref('stg_dashboard__contained_level_answers') }}
),

school_years as (
    select * 
    from {{ref('int_school_years')}}
),

combined as (
    select distinct
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
            when ss.question_type = 'freeresponse'
            then coalesce(lsfr.data,ls.data) 
            else ant.answer_text
        end as answer_response -- free response answer or selected answer

        , ss.num_response_options
        , ul.user_level_id      -- useful to count unique submissions
        , ul.level_source_id    -- useful if there's a need to join to level_sources 

    from student_surveys    as ss 

    join user_levels   as ul 
     on ss.script_id            = ul.script_id
    and ss.contained_level_id   = ul.level_id

    left join level_sources as ls 
     on ul.level_source_id = ls.level_source_id

    left join free_responses as lsfr 
    on ul.level_source_id = lsfr.level_sources_free_response_id

    left join answer_texts as ant
    on ls.level_id = ant.level_id
    and ls.data = ant.answer_number

    join users              as usr 
    on ul.user_id = usr.user_id 

    join school_years       as sy 
    on ul.created_at 
        between sy.started_at 
            and sy.ended_at )

select * 
from combined 