CREATE
OR REPLACE PROCEDURE PUBLIC.run_course_structure() LANGUAGE plpgsql AS $$
BEGIN
  DROP TABLE if EXISTS analysis.course_structure_BUILD;
CREATE TABLE analysis.course_structure_BUILD AS (
    SELECT
      cn.course_name_short,
      cn.course_name_long,
      sn.script_name_short,
      sn.script_name_long,
      C.id course_id,
      C.name course_name,
      sl.script_id,
      sn.versioned_script_name,
      sc.name script_name,
      st.id stage_id,
      st.name stage_name,
      CASE
        WHEN lockable = 1 THEN st.absolute_position
        ELSE st.relative_position
      END stage_number,
      CASE
        WHEN sl.script_id = '26'
        AND lsl.level_id = '14633' THEN '1'
        ELSE lsl.level_id
      END AS level_id,
      --------------------- hard coded error correction, level_script_levels defines the first level of this script as id# 14,633 when user_levels defines this level as #1
      le.name level_name,
      sl.position AS level_number,
      --case when json_extract_path_text(sl.properties, 'challenge') = 'true' then 1 else 0 end as challenge,
      CASE
        WHEN sl.assessment = 1 THEN 1
        ELSE 0
      END AS assessment,
      --case when json_extract_path_text(le.properties, 'mini_rubric') = 'true' then 1 else 0 end as mini_rubric
      CASE
        WHEN script_name LIKE 'devices-20__' THEN 'csd'
        WHEN script_name LIKE '%hello%' THEN 'hoc'
        WHEN script_name LIKE 'microbit%' THEN 'csd'
        WHEN script_name LIKE 'csd-post-survey-20__' THEN 'csd'
        WHEN script_name LIKE 'csp-post-survey-20__' THEN 'csp'
        WHEN json_extract_path_text(
          sc.properties,
          'curriculum_umbrella'
        ) = '' THEN 'other'
        ELSE LOWER(
          json_extract_path_text(
            sc.properties,
            'curriculum_umbrella'
          )
        )
      END AS course_name_true,
      RANK () over (
        PARTITION BY sl.script_id
        ORDER BY
          stage_number,
          sl.position
      ) level_script_order,
      le.updated_at AS updated_at
    FROM
      dashboard_production.levels_script_levels lsl
      JOIN dashboard_production.script_levels sl
      ON sl.id = lsl.script_level_id
      JOIN dashboard_production.stages st
      ON st.id = sl.stage_id
      JOIN dashboard_production.levels le
      ON le.id = lsl.level_id
      JOIN dashboard_production.scripts sc
      ON sc.id = sl.script_id
      LEFT JOIN dashboard_production.course_scripts cs
      ON cs.script_id = sc.id
      LEFT JOIN dashboard_production.unit_groups C
      ON C.id = cs.course_id
      LEFT JOIN analysis.course_names cn
      ON cn.versioned_course_id = C.id
      LEFT JOIN analysis.script_names sn
      ON sn.versioned_script_id = sc.id
  );
-- Show output of any changes - could also capture them in a table
  -- SELECT * FROM course_structure_BUILD
  -- EXCEPT
  -- SELECT * FROM course_structure;
  -- Hotswap drop-and-rename
  DROP TABLE if EXISTS analysis.course_structure;
ALTER TABLE
  analysis.course_structure_BUILD RENAME TO course_structure;
GRANT ALL privileges
  ON analysis.course_structure TO GROUP admin;
GRANT
SELECT
  ON analysis.course_structure TO GROUP reader,
  GROUP reader_pii;
END;$$
