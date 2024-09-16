CREATE
OR REPLACE PROCEDURE PUBLIC.run_school_course_status() LANGUAGE plpgsql AS $$
BEGIN
  -----------------------------------------------------------------------------------------------------------------------------
  ----- Build something like school_rosetta for purposes of 2025 goal setting
  ----- want to see trends of new v. retained schools, etc.
  -----------------------------------------------------------------------------------------------------------------------------
  CREATE TABLE PUBLIC.bf_school_course_status_BUILD AS (
    -- DROP TABLE IF EXISTS analysis.school_course_status;
    -- CREATE TABLE analysis.school_course_status AS (
    -- DROP VIEW IF EXISTS analysis.school_course_status_view;
    -- CREATE VIEW analysis.school_course_status_view AS (
    WITH
    /* Teachers who started a course, by school, school year and course */
    schools_teachers AS (
      SELECT
        school_id,
        school_year,
        course_name,
        COUNT (
          DISTINCT CASE
            WHEN t.started = 1 THEN t.user_id
            ELSE NULL
          END
        ) num_teachers_started,
        MIN(
          t.started_at
        ) first_started_at --Baker added. started at here means earlist timestamp for any/all teachers at this school teaching this course this year,
        DENSE_RANK () over (
          PARTITION BY t.school_id,
          t.course_name
          ORDER BY
            t.school_year
        ) AS seq,
        DENSE_RANK () over (
          PARTITION BY t.school_id,
          t.course_name
          ORDER BY
            t.school_year DESC
        ) seq_inv
      FROM
        analysis.teachers t
      WHERE
        COALESCE(
          t.started,
          0
        ) >= 1
      GROUP BY
        school_id,
        school_year,
        course_name --order by school_id, course_name, school_year
    ) --
    --
    /* Students who started a course, by school year course , and teacher's school -- Not using students' school because it seems to drop a lot of students */,
    schools_students AS (
      SELECT
        t1.school_id,
        t1.school_year,
        t1.course_name,
        COUNT (
          DISTINCT sd.student_user_id
        ) num_students_started
      FROM
        analysis.teachers t1 --join analysis.students sd
        JOIN analysis.student_teacher_section_complete sd
        ON t1.school_year = sd.school_year
        AND t1.course_name = sd.course_name
        AND t1.user_id = sd.teacher_user_id -- and t1.school_id = sd.school_id
      WHERE
        COALESCE(
          t1.started,
          0
        ) >= 1
      GROUP BY
        t1.school_id,
        t1.school_year,
        t1.course_name --order by school_id, course_name, school_year
    ) --
    --
    /* cross join of school years and courses */,
    school_years_courses AS (
      SELECT
        sy.school_year,
        sy.school_year_int,
        course_name
      FROM
        analysis.school_years sy
        CROSS JOIN (
          SELECT
            DISTINCT cs.course_name_true course_name
          FROM
            analysis.course_structure cs
          WHERE
            len(
              cs.course_name_true
            ) < 5 -- excludes other, pilots and PL
        )
      WHERE
        CASE
          WHEN EXTRACT (
            MONTH
            FROM
              SYSDATE
          ) > 6 THEN sy.school_year_int <= EXTRACT(
            YEAR
            FROM
              SYSDATE
          )
          ELSE sy.school_year_int <= EXTRACT(
            YEAR
            FROM
              SYSDATE
          ) -1
        END
    ) --
    --
    /* schools with no code.org activity ever */,
    schools_no_activity AS (
      SELECT
        DISTINCT s.id
      FROM
        dashboard_production.schools s
        LEFT JOIN schools_teachers st
        ON s.id = st.school_id
      WHERE
        st.school_id IS NULL
    ) --
    --
    /* summary of teacher accounts, includes records where teachers were trained but not started */,
    schools_teachers_all AS (
      SELECT
        school_id,
        school_year,
        course_name,
        COUNT (
          DISTINCT CASE
            WHEN t.trained = 1 THEN t.user_id
            ELSE NULL
          END
        ) num_teachers_trained,
        SUM (
          t.trained
        ) num_teachers_trained_2 --Baker: double-checking this ^^^ does what I think,
        SUM(
          t.trained_this_year
        ) num_teachers_trained_this_year --Baker: added this as important qualifier for views requiring teacher_trained info.,
        COUNT (
          DISTINCT t.user_id
        ) num_teacher_accounts
      FROM
        analysis.teachers t
      WHERE
        COALESCE(
          t.started,
          0
        ) >= 1
        OR COALESCE(
          t.trained,
          0
        ) >= 1 -- /*** Added this to include trained teachers***/
      GROUP BY
        school_id,
        school_year,
        course_name
    ) --
    --
    SELECT
      DISTINCT syc.school_year --, sy.school_year_int,
      s.id,
      s.id school_id --keep id and school_id so it doesn't break dashboards.  fix later,
      s."name" school_name,
      syc.course_name -- , st.course_name --sanity check,
      sd."name" school_district_name,
      s.city,
      s.state,
      s.school_type -- , s.latitude
      -- , s.longitude
      -- , ssy.grades_lo grades_lo
      -- , ssy.grades_hi grades_hi,
      ssy.stage_el,
      ssy.stage_mi,
      ssy.stage_hi,
      ssy.students total_school_students -- , ssy.student_am
      -- , ssy.student_as
      -- , ssy.student_hi
      -- , ssy.student_hp
      -- , ssy.student_bl
      -- , ssy.student_wh
      -- , ssy.student_tr
      -- , ssy.frl_eligible,
      ssy.community_type,
      ssy.rural --Baker added this,
      ssy.high_needs -- Baker added this,
      ssy.urm_percent,
      ssy.frl_eligible_percent,
      CASE
        WHEN urm_percent >= 0.5 THEN 1
        ELSE 0
      END urg_scholarship_eligible,
      CASE
        WHEN frl_eligible_percent >= 0.5 THEN 1
        ELSE 0
      END frl_scholarship_eligible,
      CASE
        WHEN urm_percent >= 0.5 THEN 1
        WHEN frl_eligible_percent >= 0.5 THEN 1
        ELSE 0
      END scholarship_eligible --, case when asc2.school_code is null then 0 else 1 end access_report_teaches,
      st2.num_teacher_accounts,
      st2.num_teachers_trained --does this need to pull from st.teachers_trained as well?,
      st2.num_teachers_trained_this_year --baker: added this,
      st.num_teachers_started,
      st.first_started_at --baker added this,
      st.seq num_years_active -- ** School code.org status in one column *** --
      -- status is a sequence of states a school can move through.,
      CASE
        WHEN st.seq = 1 THEN 'active new'
        WHEN st.seq > 1
        AND (LAG (seq, 1) over (PARTITION BY s.id, syc.course_name
        ORDER BY
          syc.school_year)) IS NOT NULL THEN 'active retained'
          WHEN (
            st.seq IS NULL
            AND (LAG (seq, 1) over (PARTITION BY s.id, syc.course_name
            ORDER BY
              syc.school_year)) IS NOT NULL
          ) --then 'dropped_this_year'
          THEN 'inactive this year'
          WHEN (
            st.seq IS NULL
            AND (SUM(seq) over (PARTITION BY s.id, syc.course_name
            ORDER BY
              syc.school_year rows unbounded preceding)) IS NOT NULL --cumulative sum > 1
              AND (LAG (seq, 1) over (PARTITION BY s.id, syc.course_name
            ORDER BY
              syc.school_year)) IS NULL
          ) --then 'dropped_over_1year' -- School/course had started or trained teachers last year but not this year
          THEN 'inactive churn'
          WHEN st.seq > 1
          AND (LAG (seq, 1) over (PARTITION BY s.id, syc.course_name
        ORDER BY
          syc.school_year)) IS NULL THEN 'active reacquired' --else 'no_activity'
          ELSE 'market'
      END course_status --bf: changed alias from school_status --> course_status,
      CASE
        WHEN course_status LIKE '%new%' THEN TRUE
        ELSE FALSE
      END is_new --  , case when course_status LIKE '%dropped%' THEN true ELSE false END is_dropped --keep to support transition to splitting out dropped in school_summary aggregate.  remove once summary is working.,
      CASE
        WHEN course_status LIKE '%inactive%' THEN TRUE
        ELSE FALSE
      END is_inactive --  , case when course_status='dropped_over_1year' THEN true ELSE false END is_dropped_over_1year,
      CASE
        WHEN course_status = 'inactive churn' THEN TRUE
        ELSE FALSE
      END is_churn --  , case when course_status='dropped_this_year' THEN true ELSE false END is_dropped_this_year,
      CASE
        WHEN course_status = 'inactive this year' THEN TRUE
        ELSE FALSE
      END is_inactive_this_year,
      CASE
        WHEN course_status LIKE '%retained%' THEN TRUE
        ELSE FALSE
      END is_retained --  , case when course_status='restarted' THEN true else false end is_restarted,
      CASE
        WHEN course_status LIKE '%reacquired' THEN TRUE
        ELSE FALSE
      END is_reacquired,
      CASE
        WHEN st.seq IS NOT NULL THEN TRUE
        ELSE FALSE
      END is_active,
      ss.num_students_started
    FROM
      dashboard_production.schools s
      CROSS JOIN school_years_courses syc
      LEFT JOIN dashboard_production.ap_school_codes asc2
      ON s.id = asc2.school_id
      AND syc.school_year_int + 1 = asc2.school_year
      LEFT JOIN dashboard_production.school_districts sd
      ON s.school_district_id = sd.id
      LEFT JOIN analysis.school_stats ssy
      ON s.id = ssy.school_id
      LEFT JOIN schools_teachers st
      ON s.id = st.school_id
      AND syc.school_year = st.school_year
      AND syc.course_name = st.course_name -- records where teachers_started = 1 from analysis.teachers
      LEFT JOIN schools_teachers_all st2
      ON s.id = st2.school_id
      AND syc.school_year = st2.school_year
      AND syc.course_name = st2.course_name -- Joining again in case there are teachers trained but not started, those records would have been excluded from schools_teachers
      LEFT JOIN schools_students ss
      ON s.id = ss.school_id
      AND syc.school_year = ss.school_year
      AND syc.course_name = ss.course_name
    WHERE
      (
        s.id || syc.course_name IN (
          SELECT
            DISTINCT school_id || course_name
          FROM
            schools_teachers
        ) -- Filters to only courses with activity for schools that have ever used code.org curriculum
        OR s.id IN (
          SELECT
            s.id
          FROM
            schools_no_activity
        ) -- or schools with no code.org activity ever
      )
      /* test cases */
      -- and s.id in ( '00030884', '100124000242', '317458000209')
    ORDER BY
      3,
      4,
      1
  );
--end create table;
  DROP TABLE IF EXISTS bf_school_course_status;
ALTER TABLE
  PUBLIC.bf_school_course_status_BUILD RENAME TO bf_school_course_status;
GRANT ALL
  ON PUBLIC.bf_school_course_status TO GROUP admin;
GRANT
SELECT
  ON PUBLIC.bf_school_course_status TO GROUP reader_pii;
--Make a copy for analysis schema as part of transistion. Eventually make plan to deprecate the bf_version.
  CREATE TABLE analysis.school_course_status_BUILD AS (
    SELECT
      *
    FROM
      PUBLIC.bf_school_course_status
  );
DROP TABLE IF EXISTS analysis.school_course_status;
ALTER TABLE
  analysis.school_course_status_BUILD RENAME TO school_course_status;
GRANT ALL
  ON analysis.school_course_status TO GROUP admin;
GRANT
SELECT
  ON analysis.school_course_status TO GROUP reader_pii;
END;$$
