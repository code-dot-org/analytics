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
    from {{ ref('stg_dashboard__user_levels' )}} -- option: add level_source_id to dim_user_levels 
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
        ss.*,
        ul.user_id as student_id,
        ul.created_at,
        sy.school_year,
        usr.country,
        usr.us_intl,
        usr.gender,
        usr.races,
        usr.is_urg,
        case 
            when ss.question_type = 'multi' 
            then ant.answer_text
            
            when ss.question_type = 'freeresponse'
            then coalesce(lsfr.data,ls.data) 
            
        end as answer_response -- answer chosen or written by student 

    from student_surveys    as ss 
    left join user_levels   as ul 
     on ul.script_id    = ss.script_id 
    and ul.level_id     = ss.survey_level_id 

    left join level_sources as ls 
    on ul.level_source_id = ls.level_sources_id

    left join free_responses as lsfr 
    on ls.level_sources_id = lsfr.level_sources_free_response_id

    -- left join answer_texts  as ant 
    -- on  ss.contained_level_id = ant.contained_level_answers_id
    -- and lsfr.data = ant.answer_number

    join users              as usr 
    on ul.user_id = usr.user_id 

    join school_years       as sy 
    on ul.created_at 
        between sy.started_at 
            and sy.ended_at )

select 
    survey_name, 
    course_name, 
    survey_type, 
    question_number, question_text,
    answer_number, answer_text, 
    count(distinct student_id) as num_student_responses
from combined
where script_name = 'csd3-2024' 
    -- and version_year = '2024'
    and survey_type = 'end of unit'
group by 
    1,2,3,4,5,6,7