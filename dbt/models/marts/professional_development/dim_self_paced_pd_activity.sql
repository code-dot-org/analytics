with 
course_structure as (
    select 
    *
    from {{ ref('dim_course_structure') }}
    where 
        course_name != 'other'
        and participant_audience = 'teacher'
        and script_name not in ('alltheselfpacedplthings')
        and content_area like '%self_paced_pl%'
),

user_levels as (
    select *
    from {{ ref('dim_user_levels') }}
    where created_date > {{ get_cutoff_date() }} 
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

teacher_school_historical as (
    select *
    from {{ ref('int_teacher_schools_historical') }}
),

self_paced_scripts as (
    select distinct 
        cs.level_id
        , cs.script_id
        , cs.level_script_id
        , cs.stage_id
        , cs.unit
        , cs.script_name
        , cs.stage_name
        , cs.level_name
        , cs.stage_number
        , cs.level_number
        , cs.level_script_order
        , cs.course_name
        , cs.content_area
        , cs.topic_tags
        , replace(cs.course_name, 'self-paced pl - ','') as course_name_implementation
        -- , case 
        --     when cs.script_name ilike 'k5-onlinepd%' 		    then 'csf'
		-- 	when cs.script_name like 'self-paced-pl-k5%'	    then 'csf'
  		-- 	when cs.script_name like 'self-paced-pl-csd%'	    then 'csd'
  		-- 	when cs.script_name like 'self-paced-pl-csp%'	    then 'csp'
  		-- 	when cs.script_name like 'self-paced-pl-csc%'	    then 'csc'
		-- 	when cs.script_name like 'self-paced-pl-aiml%'      then 'csd'
  		-- 	when cs.script_name like 'self-paced-pl-physical%'	then 'csd'
  		-- 	when cs.script_name like 'self-paced-pl-microbit%'	then 'csd'
  		-- 	when cs.script_name like 'kodea-pd%'			    then 'csf'
        --     when cs.script_name like 'self-paced-pl-ai-101%'    then 'ai_teachers'
        --     when cs.script_name like 'k5howaimakesdecisions'    then 'csf'
        --     when cs.script_name like '%getting%started%'        then 'csf'
        --     when cs.script_name like '%foundations%'            then 'foundations'
        --     when cs.course_name in ('csf self paced pl')        then 'csf'
        --     else 'other'
  		-- 	end                                                                         as course_name_implementation
    from course_structure cs
)
select distinct
    ul.user_id                                                                          as teacher_id
    , t.us_intl
    , t.country
    , ul.level_id
    , ul.script_id
    , ul.level_script_id
    , sps.stage_id
    , sps.unit
    , sps.script_name
    , sps.stage_name
    , sps.level_name
    , sps.course_name
    , sps.content_area
    , sps.topic_tags
    , sps.course_name_implementation
    , ul.created_date                                                                   as level_created_dt
    , sy.school_year                                                                    as level_created_school_year
    , extract('year' from ul.created_date)                                              as level_created_cal_year 
    , ul.best_result
    , ul.time_spent_minutes                                                             as time_spent
    , l.level_type
    -- , t.studio_person_id
    -- , t.gender
    -- , t.races
    -- , t.is_urg
    , tsh.school_id -- school at the time of self-paced activity
    , rank () 
        over (
            partition by ul.user_id 
                order by ul.created_date asc)                     as touch_rank
    , sps.level_number
    , sps.level_script_order
    , sps.stage_number 
    -- , t.created_at                                              as account_created_at

from self_paced_scripts                                         as sps

join user_levels                                                as ul
    on sps.level_script_id = ul.level_script_id 

join levels                                                     as l 
    on ul.level_id = l.level_id

join teachers                                                   as t
    on ul.user_id = t.teacher_id

join school_years                                               as sy
    on ul.created_date 
      between sy.started_at 
        and sy.ended_at

left join teacher_school_historical as tsh 
        on
            t.teacher_id = tsh.teacher_id 
            and ul.created_date 
              between tsh.started_at 
                and tsh.ended_at 