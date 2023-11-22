/*

Design: 1 row per student, school_year, churn_status
Logic: where student has "started" (visited one level) per school year for any/all courses.
    -- for example: if a student has a start in CSF in 2021-22 and CSP for 2022-23, then they are "active retained" for 2022-23

    -- active retained: student has a course-start in the previous school year and this school year
    -- active reacquired: student does NOT have a course-start last year, but does this year and in some other prior year (e.g. yes 19-20, NO 20-21, yes 21-22)
    -- active new: never has had a course-start prior to this year (i.e. first time student has ever had a course start)
    -- inactive churn: did not have an active course-start last SY and does not have an active section this SY
    -- inactive this year: had an active course-start last SY, does not have one this SY
    -- market: has a student account but has never had an active course_start 
*/

with student_courses_started as (
    select
        student_id,
        school_year,
        listagg(distinct course_name, ', ') within group (order by course_name ASC) courses_started
    from {{ ref('dim_student_courses') }}
    group by 1, 2
)
, all_student_users as (
    select
        student_id,
        created_at
    from {{ref('dim_students')}}
)
, school_years as (
    select * from {{ref('int_school_years')}}
)
, all_students_school_years as (

    select
        u.student_id,
        sy.school_year
    from all_student_users u
    join school_years sy on u.created_at <= sy.ended_at
    where sy.ended_at <= current_timestamp

)
select
    all_sy.student_id,
    all_sy.school_year,
    case when s.student_id is null then 0 else 1 end as is_active,
    s.courses_started

from all_students_school_years all_sy
left join student_courses_started s on s.student_id = all_sy.student_id and s.school_year = all_sy.school_year
