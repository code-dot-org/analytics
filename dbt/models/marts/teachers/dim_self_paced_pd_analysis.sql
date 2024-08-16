with 

active_teachers as (
    select 
        teacher_id
        , school_year
        , course_name
        , 1 as started    
    from {{ ref('int_active_sections') }}
)

, self_paced_pd_activity as (
    select * 
    from {{ ref('dim_self_paced_pd_activity') }}
)

, teacher_history as(
    select 
        t.course_name
        , t.teacher_id
        , first_value (
            case 
                when t.started = 1 then t.school_year 
                else null 
            end ignore nulls) 
                over (
                    partition by t.teacher_id, t.course_name  
                    order by left(t.school_year,4) 
                    rows between unbounded preceding and unbounded following
                )                                                                       as first_teaching_sy
        -- , count (
        --     case 
        --         when t.started = 1 then t.school_year 
        --         else null 
        --     end ignore nulls
        -- ) 
        --     over (
        --         partition by t.teacher_id, t.course_name 
        --     )                                                                           as num_sy_teaching 
        -- , first_value (
        --     case 
        --         when t.started = 1 then t.school_id 
        --         else null 
        --     end ignore nulls) 
        --     over (
        --         partition by t.teacher_id, t.course_name  
        --         order by left(t.school_year,4) 
        --         rows between unbounded preceding and unbounded following)               as school_id_sy1
    from active_teachers                                                                as t
)

, self_paced_metrics_1 as (
    select distinct
        teacher_id
        , course_name_true
        , script_name
        , count (distinct script_id)                                                    as num_scripts
        , count (distinct level_id)                                                     as num_levels
    from self_paced_pd_activity     
    group by teacher_id
            , course_name_true
            , script_name
)

, self_paced_metrics_2 as (
    select distinct 
        teacher_id
        , course_name_true
        , script_name
        , min (level_created_at) 	
            over (
                partition by teacher_id, course_name_true)                                   as start_dt
        , max (level_created_at) 	
            over (
                partition by teacher_id, course_name_true)                                   as end_dt
        , min (level_created_school_year) 	
            over (
                partition by teacher_id, course_name_true)                                   as first_self_paced_sy  -- accounting for users who are have activity in multiple schools years for the same course
        , last_value (script_id) 
	        over (
                partition by teacher_id, course_name_true
			    order by level_created_at 
                rows between unbounded preceding and unbounded following)               as max_script_id
        , last_value (script_name) 
	        over (
                partition by teacher_id, course_name_true
			    order by level_created_at 
                rows between unbounded preceding and unbounded following)               as max_script_name
        , last_value (level_id) 
	        over (
                partition by teacher_id, course_name_true, script_name
			    order by level_created_at 
                rows between unbounded preceding and unbounded following)               as max_level_id
        , last_value (level_name) 
	        over (
                partition by teacher_id, course_name_true, script_name
			    order by level_created_at 
                rows between unbounded preceding and unbounded following)               as max_level_name
        , last_value (stage_id) 
	        over (
                partition by teacher_id, course_name_true, script_name
			    order by level_created_at 
                rows between unbounded preceding and unbounded following)               as max_stage_id
        , last_value (stage_name) 
	        over (
                partition by teacher_id, course_name_true, script_name
			    order by level_created_at 
                rows between unbounded preceding and unbounded following)               as max_stage_name
    from self_paced_pd_activity 
)

, self_paced_summary as (   
    select distinct 
        sp.teacher_id
        , sp.course_name_true
        , sp.course_name_implementation
        , sp.studio_person_id
        , m2.start_dt
        , m2.end_dt
        , m2.first_self_paced_sy
        , sp.script_name
        , m1.num_scripts
        , m1.num_levels
        , m2.max_script_id
        , m2.max_script_name
        , m2.max_stage_id
        , m2.max_stage_name
        , m2.max_level_id
        , m2.max_level_name
    from self_paced_pd_activity                                                         as sp
    left join self_paced_metrics_1                                                      as m1 
        on sp.teacher_id = m1.teacher_id 
        and sp.course_name_true = m1.course_name_true
        and sp.script_name = m1.script_name
    join self_paced_metrics_2                                                           as m2 
        on sp.teacher_id = m2.teacher_id 
        and sp.course_name_true = m2.course_name_true  
        and sp.script_name = m2.script_name
)

select distinct
    coalesce(th.teacher_id, sps.teacher_id)                                             as teacher_id
    , coalesce(th.course_name, sps.course_name)                                         as course_name
    , th.first_teaching_sy
    , case 
        when sps.teacher_id is not null then 1 
        else 0 
    end                                                                                 as did_self_paced_pl
    , sps.start_dt
    , sps.end_dt
    , sps.first_self_paced_sy
    , sps.script_name
    , sps.num_scripts
    , sps.num_levels
    , sps.max_script_id
    , sps.max_script_name
    , sps.max_stage_id
    , sps.max_stage_name
    , sps.max_level_id
    , sps.max_level_name
    , case	
        when th.first_teaching_sy is null 
            or sps.first_self_paced_sy is null 
            then null
        when left(th.first_teaching_sy,4) > left(sps.first_self_paced_sy,4) 
            then 'After self-paced'
        when left(th.first_teaching_sy,4) < left(sps.first_self_paced_sy,4) 
            then 'Before self-paced'
        when left(th.first_teaching_sy,4) = left(sps.first_self_paced_sy,4) 
            then 'Same year as self-paced'
    end                                                                                 as course_teaching_timing
from self_paced_summary                                                      as sps 
left join  teacher_history as th
    on th.teacher_id = sps.teacher_id 
    and th.course_name = sps.course_name_implementation
