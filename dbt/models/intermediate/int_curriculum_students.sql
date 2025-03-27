/* Author: Cory
Date: 11/30/24
Purpose: Used for establishing 2030 participating student goals
Description:
This int file creates a row for each school_year, student, course_or_module, and then the qualifying date (1 day for ES, 5 days for HS)

Edit log: 
*/


with

students as (
    select
        dssla.student_id
        , school_year
        , activity_date
        , content_area
        , course_name
        , dssla.country
        , dssla.us_intl
        , gender_group
        , race_group
    from
        {{ref('dim_student_script_level_activity')}} dssla
    left join 
        {{ref('dim_students')}} students
        on dssla.student_id = students.student_id
    where 
        user_type = 'student' and
        content_area <> 'hoc' and
        activity_date >= '2020-07-01'
)

, days_per_student_course as ( --groups and orders by date
    select 
    school_year
    , student_id
    , country
    , us_intl
    , race_group
    , gender_group
    , case 
        when content_area  = 'curriculum_k_5' then 'ES'
        when content_area  = 'curriculum_6_8' then 'MS'
        when content_area  = 'curriculum_9_12' then 'HS'
        else NULL
        end grade_band
    , course_name
    , activity_date
    , row_number() over (partition by student_id, school_year, course_name, grade_band order by activity_date asc) as day_order
    from students 
    group by 1,2,3,4,5,6,7,8,9
)

, qualifying_day_ES_MS as (
    select 
        school_year
        , student_id
        , grade_band
        , country
        , us_intl
        , race_group
        , gender_group
        , activity_date as qualifying_date
    from
        days_per_student_course
    where
        grade_band in ('ES','MS')
        and day_order = 1
)

, qualifying_day_HS as (
    select 
        school_year
        , student_id
        , grade_band
        , country
        , us_intl
        , race_group
        , gender_group
        , activity_date as qualifying_date
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