CREATE
OR REPLACE VIEW analysis.school_status AS (
  -- DROP VIEW IF EXISTS public.bf_school_status_summaries;
  -- CREATE OR REPLACE VIEW public.bf_school_status_summaries AS (
  -- This view aggregates school/course statuses into a status at the school-level
  -- The logic summarizes status across all courses at the school to assign it a status like 'new', 'dropped', etc.
  with first_pass_schools AS (
    SELECT
      school_id,
      school_year,
      bool_or(is_new) has_new,
      bool_or(is_retained) has_retained --    , bool_or(is_restarted) has_restarted
,
      bool_or(is_reacquired) has_reacquired --    , bool_or(is_dropped) has_dropped  --has inactive?
,
      bool_or(is_inactive) has_inactive,
      bool_or(is_active) has_active --has_active is important info for the second pass where we look back to see if a school had any active courses in the year prior
,
      case --    when has_new=true and has_dropped=false and has_retained=false and has_restarted=false THEN 'new' --active new
      when has_new = true
      and has_inactive = false
      and has_retained = false
      and has_reacquired = false THEN 'active new' --active new
      --    when has_new=false and has_dropped=false and has_active = false then 'no_activity' --market
      when has_new = false
      and has_inactive = false
      and has_active = false then 'market' --market
      else NULL end school_status --, SUM(num_students_started) num_students_active -- CAN'T DO THIS because it would double-count students who have activity in e.g. csd + csp in school_course_status
,
      listagg(
        (
          case
          when is_active = true THEN course_name END
        ),
        ','
      ) active_courses --useful for quick check on what was taught
,
      sum(num_teachers_trained) num_teachers_trained,
      sum(num_teachers_trained_this_year) num_teachers_trained_this_year,
      min(first_started_at) school_first_started_at --Baker added this
    FROM
      public.bf_school_course_status
    WHERE
      course_name <> 'hoc' -- this filter should prevent HoC from being included in school status calculations.
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  ),
  second_pass as (
    -- second pass, looks back at prev year active/inactive status and fills in status=NULL with restarted, retained, or dropped_this_year, dropped_over_1year
    SELECT
      school_id,
      school_year,
      lag(has_active, 1) over (
        partition by school_id
        order by
          school_year ASC
      ) prev_active,
      case --  look back at prev_active 0|1 and has_active 0|1 - below sets the statuses for those 4 combinations that can only be set on a second pass looking back
      when school_status IS NULL
      and prev_active = true
      and has_active = true THEN 'active retained' --active retained
      --    when school_status IS NULL and prev_active=true   and has_active=false  THEN 'dropped_this_year' --inactive this year
      when school_status IS NULL
      and prev_active = true
      and has_active = false THEN 'inactive this year'
      when school_status IS NULL
      and prev_active = false
      and has_active = true THEN 'active reacquired' --active reaquired
      --      when school_status IS NULL and prev_active=false  and has_active=false  THEN 'dropped_over_1year' -- inactive churn
      when school_status IS NULL
      and prev_active = false
      and has_active = false THEN 'inactive churn' -- inactive churn
      else school_status end school_status,
      active_courses --, num_students_active
,
      has_new --  , has_dropped  --inactive this_year + churn
,
      has_inactive,
      has_retained,
      has_active --  , has_restarted
,
      has_reacquired,
      num_teachers_trained,
      num_teachers_trained_this_year,
      school_first_started_at
    FROM
      first_pass_schools
    ORDER BY
      school_id,
      school_year
  ),
  school_student_counts AS (
    SELECT
      COUNT(DISTINCT(student_user_id)) num_students,
      school_id,
      school_year
    FROM
      analysis.student_teacher_section_complete
    WHERE
      school_id IS NOT NULL
    GROUP BY
      2,
      3
  )
  SELECT
    -- third pass only to remove prev_active field
    sp.school_id,
    sp.school_year,
    school_status,
    active_courses --  , num_students_active
,
    ssc.num_students,
    has_new --  , has_dropped  --inactive this_year + churn
,
    has_inactive,
    has_retained,
    has_active --  , has_restarted
,
    has_reacquired,
    num_teachers_trained,
    num_teachers_trained_this_year,
    school_first_started_at
  FROM
    second_pass sp
    LEFT JOIN school_student_counts ssc ON ssc.school_id = sp.school_id
    and ssc.school_year = sp.school_year
  ORDER BY
    1,
    2
) with no schema binding;