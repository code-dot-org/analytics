/*

Design: 1 row per student, school_year, churn_status
Logic: we can determine status based on three properties we can compute for every user|school_year as a binary:
    - 0/1 they are active this school_year - (A)ctive
    - 0/1 they were active in the previous school_year - (P)rev year
    - 0/1 they have ever been active in ANY school_year prior, incl. prev year - (E)ver before

    Rather than write out long complicated combinational logic in code, these 3 values can be combined 
    into an ordered 3-char string representing the concatenated true/false combinations for Active|Prev|Ever 
    e.g. "101" means: ( Active = true AND Prev year = false AND Ever before = true )
    To practice some defensive programming, we'll handle all cases exhaustively and return a 
    sentinal '<impossible status>' value as a fast-fail value for cases that we believe to be impossible.

    - '000' (0) = 'market'              -- Not active now + never been active
    - '001' (1) = 'inactive churn'      -- NOT active + NOT active prev year + active ever before
    - '010' (2) = '<impossible status>' -- should not be possible, active in the prev year should imply active ever before
    - '011' (3) = 'inactive this year'  -- NOT active + active prev year + (active ever before implied)
    - '100' (4) = 'active new'          -- active this year + NOT active last year + NOT active ever before
    - '101' (5) = 'active reacquired'   -- Active this year + NOT active last year + active in the past
    - '110' (6) = '<impossible status>' -- impossible for same reason as status (2)
    - '111' (7) = 'active retained'     -- active this year + active last year + (active ever before implied)

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
    where sy.started_at < current_timestamp

)
, active_status_simple as (
    select
        all_sy.student_id,
        all_sy.school_year,
        case when s.student_id is null then 0 else 1 end as is_active,
        s.courses_started

    from all_students_school_years all_sy
    left join student_courses_started s on s.student_id = all_sy.student_id and s.school_year = all_sy.school_year
)
, full_status AS (
    -- Determine the active status for each student in each year
    SELECT
        student_id,
        school_year,
        is_active,
        courses_started,
        coalesce(
            LAG(is_active, 1) OVER (PARTITION BY student_id ORDER BY school_year) 
            , 0
        ) AS prev_year_active,
        coalesce( --force any NULL to be 0 for this function
            MAX(is_active) OVER (PARTITION BY student_id ORDER BY school_year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
            , 0
        ) AS ever_active_before,
        (is_active || prev_year_active || ever_active_before) status_code
    FROM
        active_status_simple
)
, current_school_year AS (
    select 
        school_year
    from {{ref("int_school_years")}}
    where current_date between started_at and ended_at
)
SELECT
    student_id,
    school_year,
    courses_started,
    is_active,
    prev_year_active,
    ever_active_before,
    status_code,
    CASE 
        WHEN status_code = '000' THEN 'market'
        WHEN status_code = '001' THEN 'inactive churn'
        WHEN status_code = '010' THEN '<impossible status>'
        WHEN status_code = '011' THEN 'inactive this year'
        WHEN status_code = '100' THEN 'active new'
        WHEN status_code = '101' THEN 'active reacquired'
        WHEN status_code = '110' THEN '<impossible status>'
        WHEN status_code = '111' THEN 'active retained'
    END status
FROM
    full_status
ORDER BY
    student_id, school_year