with 
course_structure as (
    select *
    from {{ ref('dim_course_structure') }}
),

user_levels as (
    select *
    from {{ ref('stg_dashboard__user_levels') }}
),

teachers as (
    select * 
    from {{ ref('dim_teachers') }}
),

levels as (
    select * 
    from {{ ref('dim_levels') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
), 

self_paced_scripts as (
    select distinct 
        cs.level_id
        , cs.script_id
        , cs.stage_id
        , cs.unit
        , cs.script_name
        , cs.stage_name
        , cs.level_name
        , cs.stage_number
        , cs.level_number
        , cs.level_script_order
        , cs.course_name
        , case 
            when cs.script_name ilike 'k5-onlinepd%' 		    then 'csf'
			when cs.script_name like 'self-paced-pl-k5%'	    then 'csf'
  			when cs.script_name like 'self-paced-pl-csd%'	    then 'csd'
  			when cs.script_name like 'self-paced-pl-csp%'	    then 'csp'
  			when cs.script_name like 'self-paced-pl-csc%'	    then 'csc'
			when cs.script_name like 'self-paced-pl-aiml%'      then 'csd'
  			when cs.script_name like 'self-paced-pl-physical%'	then 'csd'
  			when cs.script_name like 'self-paced-pl-microbit%'	then 'csd'
  			when cs.script_name like 'kodea-pd%'			    then 'csf'
            when cs.script_name like 'self-paced-pl-ai-101%'    then 'ai_teachers'
            when cs.script_name like 'k5howaimakesdecisions'    then 'ai_k5'
            when cs.script_name like '%foundations%'            then 'foundations'
            when cs.course_name in ('csf self paced pl')        then 'csf'
  			end                                                                         as course_name_implementation
    from course_structure cs
    where 
        (
        cs.participant_audience = 'teacher'
        and cs.instruction_type = 'self_paced'
        and cs.published_state in ('stable', 'beta')
        )
        and cs.script_name not in ('alltheselfpacedplthings')
        and cs.course_name not like 'pd workshop activity%'  -- csa's self-paced pl is asynchronous work for facilitator-led pd workshops
)
select
    ul.user_id                                                                          as teacher_id
    , ul.level_id
    , ul.script_id
    , sps.stage_id
    , sps.unit
    , sps.script_name
    , sps.stage_name
    , sps.level_name
    , sps.course_name
    , sps.course_name_implementation
    , ul.created_at                                                                     as level_created_at
    , sy.school_year                                                                    as level_created_school_year
    , ul.best_result
    , ul.time_spent
    , l.level_type
    -- , t.studio_person_id
    -- , t.gender
    -- , t.races
    -- , t.is_urg
    , t.school_id
    , rank () 
        over (
            partition by ul.user_id 
                order by ul.created_at asc)                     as touch_rank
    , sps.level_number
    , sps.level_script_order
    , sps.stage_number 
    -- , t.created_at                                              as account_created_at

from self_paced_scripts                                         as sps

join user_levels                                                as ul
    on sps.level_id = ul.level_id 
    and sps.script_id = ul.script_id

join levels                                                     as l 
    on ul.level_id = l.level_id

join teachers                                                   as t
    on ul.user_id = t.teacher_id

join school_years                                               as sy
    on ul.created_at between sy.started_at and sy.ended_at
