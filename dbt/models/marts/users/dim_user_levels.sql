{# key point: though this model is driven by user_levels,
this model is a students/ mart therefore, should occur at
the level of the individual **user**...
the primary dimensions here are those relating to a user's level #}

{# maybe a related table to this could be a fct_user_levels to show
overall progress completion rates and such... #}

with
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
),

levels as (
    select * 
    from {{ ref('dim_levels')}}
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

students as (
    select * 
    from {{ ref('dim_students') }}
    where is_international = 0
),

combined as (
    select 
        ul.user_level_id,
        ul.user_id,
        ul.created_at as user_level_created_at,
        ul.level_id,
        ul.level_source_id,
        cs.stage_id,
        ul.script_id,
        sy.school_year,
        cs.course_name_true                     as course_name

        {# do we need any of this? 
        ul.best_result,
        min(ul.created_at)                      as user_level_started_at,
        max(ul.created_at)                      as user_level_ended_at,
        sum(ul.attempts)                        as total_attempts,
        count(distinct ul.level_id)             as total_levels_touched, #}

        {# rank () over (
            partition by ul.user_id, sy.school_year, cs.course_name_true
            order by ul.created_at, ul.level_script_order, ul.level_number, ul.script_id, cs.stage_id asc 
        ) as rnk_asc,

        rank () over (
            partition by ul.user_id, sy.school_year, cs.course_name_true
            order by ul.created_at, ul.level_script_order, ul.level_number, ul.script_id, cs.stage_id desc 
        ) as rnk_desc,

        count(distinct 
            case when lower(lev.type) in (
                'curriculumreference',
                'standalonevideo',
                'freeresponse',
                'external',
                'map',
                'levelgroup') 
                    then null 
                else ul.level_id 
            end)                               as course_progress_levels_touched #}

    from user_levels as ul 
     join course_structure as cs 
        on ul.script_id = cs.script_id
    join school_years as sy 
        on ul.created_at between sy.started_at and sy.ended_at
    join students as stu 
        on ul.user_id = stu.student_id
    join levels as lev 
        on ul.level_id = lev.level_id
    {{ dbt_utils.group_by(9) }}
)

select * 
from combined
