with 
course_structure as (
    select *
    from {{ ref('dim_course_structure') }}
),

user_levels as (
    select *
    from {{ ref('dim_user_levels') }}
),

section_mapping as (
    select *
    from {{ ref('int_section_mapping') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

school_status as (
    select * 
    from {{ ref('dim_school_status') }}
),

teacher_status as (
    select * 
    from {{ ref('dim_teacher_status') }}
),

schools as (
    select * 
    from {{ ref('dim_schools') }}
),

section_size as (
    select 
        section_id
        , count(distinct student_id)                                as section_size
    from {{ ref('int_section_mapping') }}
    group by section_id
)

select 
    ul.user_id                                                      as student_id
    , ul.level_id                                                   as level_id
    , ul.script_id                                                  as script_id
    , date_trunc('day', ul.created_dt)                              as activity_date

    -- time variables 
    , date_trunc ('month', ul.created_dt)                           as activity_month  
    , case 
        when extract ('month' from ul.created_dt) in (7,8,9) then 'Q1'
        when extract ('month' from ul.created_dt) in (10,11,12) then 'Q2'
        when extract ('month' from ul.created_dt) in (1,2,3) then 'Q3'
        when extract ('month' from ul.created_dt) in (4,5,6) then 'Q4'
      end                                                           as school_year_quarter
    , sy.school_year                                                as activity_school_year

    -- course, script, unit, lesson, and level characteristics
    , cs.level_name                                                 as level_name
    , cs.level_type                                                 as level_type
    , cs.script_name_long                                           as script_name
    , cs.course_name_true                                           as course_name
    , cs.stage_name                                                 as lesson_name

    -- section/teacher characteristics
    , sm.section_id                                                 as section_id
    , sm.teacher_id                                                 as section_teacher_id
    , ssi.section_size                                              as section_size
    , ts.status                                                     as teacher_status

    -- school characteristics
    , ss.status                                                     as school_status
    , sch.school_name                                               as school_name
    , sch.school_district_id                                        as school_district_id
    , sch.school_district_name                                      as school_district_name
    , sch.state                                                     as school_state
    , sch.school_type                                               as school_type
    , sch.is_stage_el                                               as school_is_stage_el
    , sch.is_stage_mi                                               as school_is_stage_mi
    , sch.is_stage_hi                                               as school_is_stage_hi
    , sch.is_high_needs                                             as school_is_high_needs
    , sch.is_rural                                                  as school_is_rural

    -- other
    , ul.us_intl                                                    as us_intl
    , ul.country                                                    as activity_country

    -- aggregates
    , max(ul.attempts)                                              as total_attempts
    , max(best_result)                                              as best_result
    , sum(time_spent)                                               as minutes_spent

from user_levels                                                    as ul

left join school_years                                              as sy
    on ul.created_dt between sy.started_at and sy.ended_at 

left join course_structure                                          as cs
    on ul.level_id = cs.level_id
    and ul.script_id = cs.script_id

left join section_mapping                                           as sm
    on ul.user_id = sm.student_id
    and ul.created_dt between student_added_at and student_removed_at

left join section_size                                              as ssi
    on ssi.section_id = sm.section_id

left join teacher_status                                            as ts 
    on sm.teacher_id = ts.teacher_id 
    and sy.school_year = ts.school_year

left join school_status                                             as ss 
    on sm.school_id = ss.school_id 
    and sy.school_year = ss.school_year

left join schools                                                   as sch
    on sm.school_id = sch.school_id  

where cs.participant_audience = 'student'
{{ dbt_utils.group_by(29) }}
limit 100
