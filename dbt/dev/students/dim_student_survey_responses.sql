-- model: dim_student_survey_responses 

with 
student_surveys as (
    select * 
    from {{ ref('dim_student_surveys') }}
),

level_sources as (
    select * 
    from {{ ref('stg_dashboard_pii__level_sources') }}
),

user_levels as (
    select * 
    from {{ ref('dim_user_levels' )}}
    where level_id in (select contained_level_id from student_surveys)
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
        level_id, answer_number, answer_text
    from {{ ref('stg_dashboard__contained_level_answers') }}
),

combined as (
    select 
        ss.*,
        ul.user_id as student_id,
        ul.created_date,
        ul.school_year,
        usr.country,
        usr.us_intl,
        usr.gender,
        usr.races,
        usr.is_urg,
        case 
            when question_type = 'multi' then ant.answer_text
            when question_type = 'freeresponse' 
            then fr.data -- coalesce(ls.data,fr.data)
        end as answer_text

    from student_surveys    as ss 
    
    join user_levels        as ul 
    on ss.script_id = ul.script_id 
    and ss.contained_level_id = ul.level_id 

    -- left join level_sources as ls 
    -- on ul.level_source_id = ls.level_source_id

    left join free_responses as fr 
    on ls.level_source_id = fr.level_source_id

    left join answer_texts  as ant 
    on ls.level_id = ant.contained_level_id
    and ls.data = ant.answer_number

    join users              as usr 
    on ul.user_id = usr.user_id )

select * from combined

