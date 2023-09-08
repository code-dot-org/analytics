CREATE
OR REPLACE PROCEDURE PUBLIC.run_user_surveys() LANGUAGE plpgsql AS $$
BEGIN
    -- public.nzm_view_user_surveys source
    DROP TABLE if EXISTS analysis.user_surveys_BUILD;
CREATE TABLE analysis.user_surveys_BUILD AS (
        --create or replace view public.nzm_view_user_surveys as (
        WITH answer_texts AS (
            SELECT
                DISTINCT level_id,
                answer_number,
                answer_text
            FROM
                dashboard_production.contained_level_answers
        ) --
        SELECT
            DISTINCT pl.id parent_level_id,
            pl.name parent_level_name,
            cl.id child_level_id,
            cl.name child_level_name,
            ul.created_at,
            sy.school_year user_response_school_year,
            CASE
                WHEN LOWER(
                    s.name
                ) LIKE '%preview-2021%' THEN '2020-21'
                WHEN LOWER(
                    s.name
                ) LIKE '%csp1-pilot%' THEN '2019-20'
                WHEN RIGHT(
                    pl.name,
                    4
                ) IN (
                    '2016',
                    '2017',
                    '2018',
                    '2019',
                    '2020',
                    '2021',
                    '2022'
                ) THEN RIGHT(pl.name, 4) || '-' ||(((RIGHT(pl.name, 2) :: INT) + 1) :: VARCHAR)
                WHEN LOWER(
                    pl.name
                ) LIKE '%2021%' THEN '2021-22'
                WHEN LOWER(
                    pl.name
                ) LIKE '%2020%' THEN '2020-21'
                WHEN LOWER(
                    pl.name
                ) LIKE '%2019%' THEN '2019-20'
                WHEN LOWER(
                    pl.name
                ) LIKE '%2018%' THEN '2018-19'
                WHEN LOWER(
                    pl.name
                ) LIKE '%2017%' THEN '2017-18'
                WHEN LOWER(
                    pl.name
                ) LIKE '%2016%' THEN '2016-17'
                WHEN LOWER(
                    pl.name
                ) LIKE '%21_22%' THEN '2021-22'
                WHEN LOWER(
                    s.name
                ) LIKE '%1819%' THEN '2018-19'
                WHEN RIGHT(
                    s.name,
                    4
                ) IN (
                    '2016',
                    '2017',
                    '2018',
                    '2019',
                    '2020',
                    '2021',
                    '2022'
                ) THEN RIGHT(s.name, 4) || '-' ||(((RIGHT(s.name, 2) :: INT) + 1) :: VARCHAR)
                ELSE NULL
            END script_level_school_year,
            cl."type" item_type,
            plcl."position",
            s.id script_id,
            s."name" script_name,
            cs.course_name_true,
            ul.user_id,
            rhsw.teacher_user_id,
            rhsw.section_id,
            rhsw.teacher_trained,
            ul.level_source_id,
            CASE
                WHEN cl.type = 'Multi' THEN json_extract_path_text(
                    json_extract_array_element_text(json_extract_path_text(cl.properties, 'questions'), 0, TRUE),
                    'text'
                )
                WHEN cl.type = 'FreeResponse' THEN json_extract_path_text(
                    cl.properties,
                    'long_instructions'
                )
                WHEN cl.type = 'External' THEN json_extract_path_text(
                    cl.properties,
                    'markdown'
                )
            END questions,
            cla.answer_number,
            CASE
                WHEN cl."type" = 'Multi' THEN cla.answer_text
                WHEN cl."type" = 'FreeResponse' THEN COALESCE(
                    lsfr.data,
                    lsmt."data"
                ) -- Changed 03/06/2023 to include Free Responses in End of Unit Surveys
            END answer_text,
            CASE
                WHEN (LOWER(s."name") LIKE '%post%'
                OR LOWER(pl.name) LIKE '%post%') THEN 'Post'
                WHEN (LOWER(s."name") LIKE '%pulse%'
                OR LOWER(pl.name) LIKE '%pulse%') THEN 'Pulse'
                WHEN (
                    (LOWER(s."name") LIKE '%pre%'
                    AND LOWER(s."name") NOT LIKE '%preview%')
                    OR (LOWER(pl.name) LIKE '%pre%'
                    AND LOWER(pl.name) NOT LIKE '%preview%')
                ) THEN 'Pre'
                WHEN LOWER(
                    pl.name
                ) LIKE '%end of unit%' THEN 'End of Unit' -- Changed 03/06/2023 to include End of Unit Surveys
                ELSE NULL
            END survey_type,
            u.gender,
            u.races,
            u.urm,
            u.birthday,
            EXTRACT (
                days
                FROM
                    (((sy.started_at - u.birthday)) / 365.25)
            ) age_sy_start,
            CASE
                WHEN cl.type = 'Multi' THEN json_array_length(json_extract_path_text(cl.properties, 'answers'))
                WHEN cl.type = 'FreeResponse' THEN 1
                WHEN cl.type = 'External' THEN 0
            END num_response_options
        FROM
            dashboard_production.parent_levels_child_levels plcl
            JOIN dashboard_production.levels cl
            ON cl.id = plcl.child_level_id
            JOIN dashboard_production.levels pl
            ON pl.id = plcl.parent_level_id
            JOIN dashboard_production.levels_script_levels lsl
            ON lsl.level_id = pl.id -- These allows to unduplicate and get only the parent level that is in the script associated with the response, from user_levels
            JOIN dashboard_production.script_levels sl
            ON lsl.script_level_id = sl.id
            JOIN dashboard_production.scripts s
            ON s.id = sl.script_id
            JOIN dashboard_production.user_levels ul
            ON ul.level_id = plcl.child_level_id
            AND ul.script_id = sl.script_id -- left join dashboard_production.level_sources_multi_types lsmt on ul.level_source_id = lsmt.level_source_id -- commented out and replaced with rpw below because this table doesn't have all the data (missing csd post survey for 2020-21)
            LEFT JOIN dashboard_production_pii.level_sources lsmt
            ON ul.level_source_id = lsmt.id
            LEFT JOIN answer_texts cla
            ON lsmt.level_id = cla.level_id
            AND lsmt."data" = cla.answer_number
            LEFT JOIN dashboard_production.level_sources_free_responses lsfr
            ON ul.level_source_id = lsfr.id
            LEFT JOIN dashboard_production_pii.users u
            ON ul.user_id = u.id
            LEFT JOIN dashboard_production_pii.user_geos ug
            ON u.id = ug.user_id
            LEFT JOIN analysis.school_years sy
            ON ul.created_at BETWEEN sy.started_at
            AND sy.ended_at
            LEFT JOIN analysis.course_structure cs
            ON ul.script_id = cs.script_id
            AND pl.id = cs.level_id
            LEFT JOIN PUBLIC.rosetta_historic_students_wip rhsw
            ON ul.user_id = rhsw.student_user_id
            AND sy.school_year = rhsw.school_year
            AND cs.course_name_true = rhsw.course_name --
        WHERE
            LOWER(
                pl.name
            ) LIKE '%survey%'
            AND s.name NOT IN (
                'allthesurveys',
                'allthethings',
                'csp-pre-survey-test-2017'
            )
            AND cs.course_name_true IN (
                'csp',
                'csd',
                'csa'
            )
            AND ug.country = 'United States'
    );
DROP TABLE if EXISTS analysis.user_surveys;
ALTER TABLE
    analysis.user_surveys_BUILD RENAME TO user_surveys;
GRANT ALL privileges
    ON analysis.user_surveys TO GROUP admin;
GRANT
SELECT
    ON analysis.user_surveys TO GROUP reader,
    GROUP reader_pii;
END;$$
