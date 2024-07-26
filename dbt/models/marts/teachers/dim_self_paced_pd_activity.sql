with 
course_structure as (
    select *
    from {{ ref('dim_course_structure') }}
),

user_levels as (
    select *
    from {{ ref('dim_user_levels') }}
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
        , cs.script_name
        , cs.stage_name
        , cs.level_name
        , cs.stage_number
        , cs.level_number
        , cs.level_script_order
        , cs.course_name_true
        , case 
            when cs.script_name LIKE 'k5-onlinepd%' 		    then 'csf'
			when cs.script_name like 'self-paced-pl-k5%'	    then 'csf'
  			when cs.script_name LIKE 'self-paced-pl-csd%'	    then 'csd'
  			when cs.script_name LIKE 'self-paced-pl-csp%'	    then 'csp'
  			when cs.script_name LIKE 'self-paced-pl-csc%'	    then 'csc'
  			when cs.script_name LIKE 'self-paced-pl-aiml%'	    then 'aiml'
			when cs.script_name like 'self-paced-pl-ai-101%'    then 'ai for teachers'
  			when cs.script_name LIKE 'self-paced-pl-physical%'	then 'maker: cp'
  			when cs.script_name LIKE 'self-paced-pl-microbit%'	then 'maker: mb'
  			when cs.script_name like 'kodea-pd%'			    then 'csf'
  			end                                                 as self_paced_pl_course
    from course_structure cs
    where 
        (
            cs.script_name like 'k5-onlinepd-20__'
            or cs.script_name like 'self-paced-pl%'
            or cs.script_name like 'kodea-pd%'  -- Translated version of csf in Spanish, for Chilean parter Kodea
        )
        and cs.script_name not like 'self-paced-pl-csd6-2021'
        and cs.script_name not like 'self-paced-pl-csa%'  -- csa's self-paced pl is asynchronous work for facilitator-led pd workshops
)
select
    ul.user_id
    , ul.level_id
    , ul.script_id
    , sps.script_name
    , sps.stage_name
    , sps.level_name
    , ul.created_dt
    , ul.updated_dt
    , ul.best_result
    , ul.time_spent
    , l.level_type
    , t.studio_person_id
    , t.gender
    , t.races
    , t.is_urg
    , t.school_info_id
    , rank () 
        over (
            partition by ul.user_id 
                order by ul.created_dt asc)                     as touch_rank
    , sps.level_number
    , sps.level_script_order
    , sps.stage_number 
    , t.created_at                                              as account_created_at

from self_paced_scripts                                         as sps

join user_levels                                                as ul
    on sps.level_id = ul.level_id 
    and sps.script_id = ul.script_id

join levels                                                     as l 
    on ul.level_id = l.level_id

join teachers                                                   as t
    on ul.user_id = t.teacher_id

limit 100

