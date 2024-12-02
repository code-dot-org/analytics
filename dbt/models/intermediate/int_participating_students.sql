/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Description:
This int file creates a row for each school_year, student, course_or_module, and then the qualifying date (1 day for ES, 5 days for HS)

Edit log: 
*/


with

dssla as (
    select
        student_id,
        school_year,
        activity_date,
        course_name,
        unit_name,
        country,
        user_type
    from {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        country = 'united states' and 
        course_name in
            ('csf','csc k-5',
            'csd','6-8 special topics','csc 6-8',
            'csa','csp','9-12 special topics','foundations of cs')
)

-- NZM provided this logic
, standalone_modules as (
    select distinct course_name, unit
    from {{ref('dim_course_structure')}} cs 
    where 
    cs.unit in (
            'csa-consumer-review-lab'
            , 'csa-data-lab'
            , 'csa-labs'
            , 'csa-magpie-lab'
            , 'csa-postap-se-and-computer-vision'
            , 'csa-software-engineering'
        )
    or (
        cs.course_name in ('csd', 'csa')
        and cs.is_active_student_course = 1
        and is_standalone = 'true'
        and cs.unit not like 'tess-test-csa'
        )
    )

, days_per_student_course as ( --groups and orders by date
    select 
    dssla.school_year
    , dssla.student_id
    , case 
        when dssla.course_name in ('csa','csp','9-12 special topics','foundations of cs') then 'HS'
        when dssla.course_name in ('csd','6-8 special topics','csc 6-8') then 'MS'
        else 'ES'
        end grade_band
    , coalesce(sm.unit, dssla.course_name) course_or_module
    , activity_date
    , row_number() over (partition by student_id, school_year, course_or_module order by activity_date asc) as day_order
    from dssla 
    left join standalone_modules sm 
        on dssla.course_name = sm.course_name and dssla.unit_name = sm.unit
    group by 1,2,3,4,5
)

, qualifying_day_ES_MS as (
    select 
        school_year,
        student_id,
        grade_band,
        course_or_module,
        activity_date as qualifying_date
    from
        days_per_student_course
    where
        grade_band in ('ES','MS')
        and day_order = 1
)

, qualifying_day_HS as (
    select 
        school_year,
        student_id,
        grade_band,
        course_or_module,
        activity_date as qualifying_date
    from
        days_per_student_course
    where
        grade_band in ('HS')
        and day_order = 5
)
select * from 
    qualifying_day_ES_MS
UNION all
select * from 
    qualifying_day_HS