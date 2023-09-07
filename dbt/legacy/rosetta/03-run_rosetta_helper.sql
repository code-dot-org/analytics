-- Migration Status:
-- relevant work has been migrated, 
-- holding on us_start, _end,
-- surfacing ul_days in "fct_students"



{# CREATE
OR REPLACE PROCEDURE PUBLIC.rosetta_helper() LANGUAGE plpgsql AS $$
DECLARE
  --  start_time timestamp;
  --  elapsed_time integer;
BEGIN
  -- Create table ul_start
  RAISE info 'Creating table ul_start...';
--  start_time := CURRENT_TIMESTAMP;
INSERT INTO
  analysis.rosetta_error_log (
    log_time,
    procedure_name,
    message,
    runtime,
    status
  )
VALUES
  (
    CURRENT_TIMESTAMP,
    'rosetta_helper',
    'Creating ul_start...',
    0,
    ''
  );
COMMIT; #}


DROP TABLE IF EXISTS analysis.ul_start;
CREATE TABLE analysis.ul_start AS (
    SELECT
      user_id,
      school_year,
      created_at AS act_dt,
      course_name,
      script_id,
      stage_id,
      level_id
    FROM
      (
        SELECT
          ul.user_id,
          sy.school_year,
          ul.created_at,
          cs.course_name_true AS course_name,
          cs.script_id,
          cs.stage_id,
          cs.level_id,
          RANK () over (
            PARTITION BY user_id,
            school_year,
            course_name_true
            ORDER BY
              ul.created_at,
              level_script_order,
              level_number,
              ul.script_id,
              cs.stage_id
          ) AS rnk -----------------script id ordering
        FROM
          dashboard_production.user_levels ul
          JOIN analysis.course_structure cs ----------------  Inner join because we don't care about levels not in course_structure
          ON ul.script_id = cs.script_id
          AND ul.level_id = cs.level_id
          JOIN analysis.school_years sy
          ON ul.created_at BETWEEN sy.started_at
          AND sy.ended_at --------------  In order to select one record per user per course per year we need to attach the activty to a school_year
          JOIN dashboard_production.users u
          ON u.id = ul.user_id
          AND u.user_type = 'student'
        WHERE
          ul.attempts > 0 -- 0 attempts are ul records we want to ignore
      )
    WHERE
      rnk = 1
  );


-- end ul_start
  --  elapsed_time := extract(epoch from CURRENT_TIMESTAMP - start_time)::integer;
{# INSERT INTO
  analysis.rosetta_error_log (
    log_time,
    procedure_name,
    message,
    runtime,
    status
  )
VALUES
  (
    CURRENT_TIMESTAMP,
    'rosetta_helper',
    'Finished ul_start',
    0,
    'success'
  ),
  (
    CURRENT_TIMESTAMP,
    'rosetta_helper',
    'Creating ul_end...',
    0,
    ''
  );
COMMIT;


-- Create table ul_end
  RAISE info 'Creating table ul_end...';
-- start_time := CURRENT_TIMESTAMP; #}


-- holding on these, it appears to just be surfacing when ea user_level
-- starts or ends which can be done elsewhere
  DROP TABLE IF EXISTS analysis.ul_end;
CREATE TABLE analysis.ul_end AS (
    SELECT
      user_id,
      school_year,
      created_at AS act_dt,
      course_name,
      script_id,
      stage_id,
      level_id
    FROM
      (
        SELECT
          ul.user_id,
          sy.school_year,
          ul.created_at,
          cs.course_name_true AS course_name,
          cs.script_id,
          cs.stage_id,
          cs.level_id,
          RANK () over (
            PARTITION BY user_id,
            school_year,
            course_name_true
            ORDER BY
              ul.created_at DESC,
              level_script_order DESC,
              level_number DESC,
              ul.script_id DESC,
              cs.stage_id DESC
          ) AS rnk -----------------script id ordering
        FROM
          dashboard_production.user_levels ul
          JOIN analysis.course_structure cs
          ON ul.script_id = cs.script_id
          AND ul.level_id = cs.level_id
          JOIN analysis.school_years sy
          ON ul.created_at BETWEEN sy.started_at
          AND sy.ended_at
        WHERE
          ul.attempts > 0
      )
    WHERE
      rnk = 1
  );
--  elapsed_time := extract(epoch from CURRENT_TIMESTAMP - start_time)::integer;
{# INSERT INTO
  analysis.rosetta_error_log (
    log_time,
    procedure_name,
    message,
    runtime,
    status
  )
VALUES
  (
    CURRENT_TIMESTAMP,
    'rosetta_helper',
    'Finished ul_end',
    0,
    'success'
  ),
  (
    CURRENT_TIMESTAMP,
    'rosetta_helper',
    'Creating ul_days...',
    0,
    ''
  );
COMMIT;
RAISE info 'Creating table ul_days...';
--  start_time := CURRENT_TIMESTAMP; #}
  DROP TABLE if EXISTS analysis.ul_days;
CREATE TABLE analysis.ul_days AS (
    SELECT
      ul.user_id,
      -- rhsw.section_id,
      course_name_true AS course_name,
      school_year,
      TO_DATE(
        ul.created_at,
        'yyyy-mm-dd'
      ) dt,
      COUNT(
        DISTINCT ul.level_id
      ) all_levels_touched,
      SUM(time_spent) time_spent,   -- pushing these to fct_students or the like 
      COUNT(
        DISTINCT (
          CASE
            WHEN l.type IN (
              'CurriculumReference',
              'StandaloneVideo',
              'FreeResponse',
              'External',
              'Map',
              'LevelGroup'
            ) THEN NULL --------- UPDATE THIS WHEN LEVEL_TYPES CHANGE!!!!!  NO CSF LEVELS ARE CURRENTLY INCLUDED
            ELSE ul.level_id
          END
        )
      ) course_progress_levels_touched
    FROM
      dashboard_production.user_levels ul
      JOIN analysis.course_structure cs
      ON ul.level_id = cs.level_id
      AND ul.script_id = cs.script_id --  limit to decided level types
      JOIN dashboard_production.user_geos ug
      ON ul.user_id = ug.user_id
      AND ug.country = 'United States' --  US only
      JOIN dashboard_production_pii.users u
      ON ul.user_id = u.id
      AND u.user_type = 'student' ---- filter to students only
      JOIN analysis.school_years sy
      ON ul.created_at BETWEEN sy.started_at
      AND sy.ended_at
      JOIN dashboard_production.levels l
      ON l.id = ul.level_id -- join public.rosetta_historic_students_wip rhsw on ul.user_id = rhsw.student_user_id and cs.course_name_true = rhsw.course_name and sy.school_year = rhsw.school_year
      -- left join section_stats ss on rhsw.section_id = ss.section_id and rhsw.course_name =
    WHERE
      ul.attempts > 0
    GROUP BY
      1,
      2,
      3,
      4
  {# );
--  elapsed_time := extract(epoch from CURRENT_TIMESTAMP - start_time)::integer;
INSERT INTO
  analysis.rosetta_error_log (
    log_time,
    procedure_name,
    message,
    runtime,
    status
  )
VALUES
  (
    CURRENT_TIMESTAMP,
    'rosetta_helper',
    'Finished ul_days',
    0,
    'success'
  );
COMMIT;
-- Add helpful messages
  RAISE info 'Tables created successfully!';
END;$$ #}
