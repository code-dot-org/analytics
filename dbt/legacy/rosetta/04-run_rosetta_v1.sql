CREATE
OR REPLACE PROCEDURE PUBLIC.run_rosetta_v1() LANGUAGE plpgsql AS $$
BEGIN
    --drop table public.rosetta_historic_teachers;
    CREATE TABLE PUBLIC.rosetta_historic_teachers_BUILD AS (
        WITH historic_teachers AS (
            SELECT
                school.*,
                csf.csf_trained,
                csd.csd_trained,
                csp.csp_trained,
                -------------
                csa.csa_trained,
                --add this column for csa_trained
                -------------
                race.race
            FROM
                (
                    SELECT
                        DISTINCT u.id user_id,
                        usi.school_info_id,
                        si.school_id,
                        COALESCE(
                            si.school_name,
                            s.name
                        ) AS school_name -------------  School infos table is more accurate than schools,
                        usi.start_date AS rec_eff_dt,
                        CASE
                            WHEN usi.end_date IS NOT NULL THEN end_date
                            ELSE '9999-12-31'
                        END AS rec_cncl_dt
                    FROM
                        dashboard_production.users u
                        JOIN dashboard_production_pii.user_school_infos usi
                        ON u.id = usi.user_id
                        AND u.user_type = 'teacher'
                        JOIN dashboard_production.user_geos ug
                        ON u.id = ug.user_id
                        AND ug.country = 'United States'
                        JOIN dashboard_production.school_infos si
                        ON si.id = usi.school_info_id
                        LEFT JOIN dashboard_production.schools s
                        ON si.school_id = s.id
                ) school
                LEFT JOIN (
                    SELECT
                        user_id,
                        MIN(trained_at) csf_trained
                    FROM
                        analysis.csf_teachers_trained
                    GROUP BY
                        1
                ) csf
                ON school.user_id = csf.user_id
                LEFT JOIN (
                    SELECT
                        u.id AS user_id,
                        MIN(
                            TO_DATE(SUBSTRING(tt.school_year, 1, 4) || '-07-01', 'YYYY-MM-DD')
                        ) AS csd_trained
                    FROM
                        analysis.csp_csd_teachers_trained tt
                        LEFT JOIN dashboard_production_pii.users u
                        ON tt.studio_person_id = u.studio_person_id
                    WHERE
                        tt.course = 'CS Discoveries'
                    GROUP BY
                        1
                ) csd
                ON school.user_id = csd.user_id
                LEFT JOIN (
                    SELECT
                        u.id AS user_id,
                        MIN(
                            TO_DATE(SUBSTRING(tt.school_year, 1, 4) || '-07-01', 'YYYY-MM-DD')
                        ) AS csp_trained
                    FROM
                        analysis.csp_csd_teachers_trained tt
                        LEFT JOIN dashboard_production_pii.users u
                        ON tt.studio_person_id = u.studio_person_id
                    WHERE
                        tt.course = 'CS Principles'
                    GROUP BY
                        1
                ) csp
                ON school.user_id = csp.user_id ----------------join to csp_csd_teachers to create column for csa_trained.  Note: this table is carrying CSA trained data---------
                LEFT JOIN (
                    SELECT
                        u.id AS user_id,
                        MIN(
                            TO_DATE(SUBSTRING(tt.school_year, 1, 4) || '-07-01', 'YYYY-MM-DD')
                        ) AS csa_trained
                    FROM
                        analysis.csp_csd_teachers_trained tt
                        LEFT JOIN dashboard_production_pii.users u
                        ON tt.studio_person_id = u.studio_person_id
                    WHERE
                        tt.course = 'Computer Science A'
                    GROUP BY
                        1
                ) csa
                ON school.user_id = csa.user_id ----------------------------------------------------------------------------------------------------------------------------------
                LEFT JOIN (
                    SELECT
                        user_id,
                        race
                    FROM
                        (
                            SELECT
                                user_id,
                                json_extract_path_text(
                                    form_data,
                                    'race'
                                ) race,
                                RANK () over (
                                    PARTITION BY user_id
                                    ORDER BY
                                        created_at DESC
                                ) rnk
                            FROM
                                dashboard_production_pii.pd_applications
                            WHERE
                                race <> ''
                                AND race <> '["American Indian/Alaska Native","Asian","Black or African American","Hispanic or Latino","White","Native Hawaiian or other Pacific Islander","Other"]'
                        ) pd
                    WHERE
                        rnk = 1
                        AND race NOT LIKE '%Prefer not to say%'
                        AND race NOT LIKE '%Prefer not to answer%'
                ) race
                ON school.user_id = race.user_id
        )
        SELECT
            teacher_user_id || school_year AS ht_id,
            teacher_user_id,
            school_info_id,
            school_id,
            school_name,
            ht.school_year,
            ht.race,
            csf_trained,
            csd_trained,
            csp_trained,
            ----new---
            csa_trained,
            -----------
            csf_started,
            csd_started,
            csp_started,
            ---new---
            csa_started ---------
        FROM
            (
                SELECT
                    ht.user_id AS teacher_user_id,
                    school_info_id,
                    school_id,
                    school_name,
                    sy.school_year,
                    race,
                    CASE
                        WHEN csf_trained < ADD_MONTHS(
                            sy.ended_at,
                            -1
                        ) THEN 1
                        ELSE 0
                    END AS csf_trained,
                    ---------------- May instead of June for CSF
                    CASE
                        WHEN csd_trained < sy.ended_at THEN 1
                        ELSE 0
                    END AS csd_trained,
                    --[baker] yipes! using csd_trained as a date, and re-writing the alias as an int 0|1
                    CASE
                        WHEN csp_trained < sy.ended_at THEN 1
                        ELSE 0
                    END AS csp_trained,
                    -------------------create csa_trained 0|1 flag-------------
                    CASE
                        WHEN csa_trained < sy.ended_at THEN 1
                        ELSE 0
                    END AS csa_trained,
                    ------------------------------------------------------------
                    CASE
                        WHEN csf.user_id IS NOT NULL THEN 1
                        ELSE 0
                    END AS csf_started,
                    CASE
                        WHEN csd.user_id IS NOT NULL THEN 1
                        ELSE 0
                    END AS csd_started,
                    CASE
                        WHEN csp.user_id IS NOT NULL THEN 1
                        ELSE 0
                    END AS csp_started,
                    -------------------create csa_started 0|1 flag-------------
                    CASE
                        WHEN csa.user_id IS NOT NULL THEN 1
                        ELSE 0
                    END AS csa_started,
                    ------------------------------------------------------------
                    RANK () over (
                        PARTITION BY ht.user_id,
                        sy.school_year
                        ORDER BY
                            school_id,
                            ht.rec_cncl_dt DESC
                    ) rnk
                FROM
                    historic_teachers ht
                    JOIN analysis.school_years sy
                    ON ht.rec_eff_dt < sy.ended_at
                    AND ht.rec_cncl_dt > sy.started_at
                    AND sy.started_at < CURRENT_DATE -------------------add a join to csp_csd_started to pull csa starts (csa added to csp_csd_started VIEW as part of this modification)-------------
                    LEFT JOIN (
                        SELECT
                            DISTINCT user_id,
                            school_year
                        FROM
                            analysis.csp_csd_started_teachers
                        WHERE
                            course_name = 'csa'
                    ) csa
                    ON ht.user_id = csa.user_id
                    AND sy.school_year = csa.school_year -------------------------------------------------------------------------------------------------------------------------------------------------
                    LEFT JOIN (
                        SELECT
                            DISTINCT user_id,
                            school_year
                        FROM
                            analysis.csp_csd_started_teachers
                        WHERE
                            course_name = 'csp'
                    ) csp
                    ON ht.user_id = csp.user_id
                    AND sy.school_year = csp.school_year
                    LEFT JOIN (
                        SELECT
                            DISTINCT user_id,
                            school_year
                        FROM
                            analysis.csp_csd_started_teachers
                        WHERE
                            course_name = 'csd'
                    ) csd
                    ON ht.user_id = csd.user_id
                    AND sy.school_year = csd.school_year
                    LEFT JOIN (
                        SELECT
                            DISTINCT user_id,
                            school_year
                        FROM
                            analysis.csf_started_teachers
                    ) csf
                    ON ht.user_id = csf.user_id
                    AND sy.school_year = csf.school_year
            ) ht
        WHERE
            rnk = 1
    );
DROP TABLE PUBLIC.rosetta_historic_teachers;
ALTER TABLE
    PUBLIC.rosetta_historic_teachers_BUILD RENAME TO rosetta_historic_teachers;
GRANT ALL
    ON PUBLIC.rosetta_historic_teachers TO GROUP admin;
GRANT
SELECT
    ON PUBLIC.rosetta_historic_teachers TO GROUP reader_pii;
CREATE temporary TABLE cs AS (
        SELECT
            *
        FROM
            analysis.course_structure
    );
--drop table ul_start;
    -- create temporary table ul_start as (
    -- select user_id, school_year, created_at act_dt, course_name, script_id, stage_id, level_id from (
    -- 	select ul.user_id, sy.school_year, ul.created_at, cs.course_name_true as course_name, cs.script_id, cs.stage_id, cs.level_id, rank () over (partition by user_id, school_year, course_name_true order by ul.created_at, level_script_order, level_number, ul.script_id, cs.stage_id) rnk  -----------------script id ordering
    -- 	from dashboard_production.user_levels ul
    -- 	join cs ----------------  Inner join because we don't care about levels not in course_structure
    -- 	on ul.script_id = cs.script_id and ul.level_id = cs.level_id
    -- 	join analysis.school_years sy on ul.created_at between sy.started_at and sy.ended_at --------------  In order to select one record per user per course per year we need to attach the activty to a school_year
    -- 	where ul.attempts > 0
    -- 	)
    -- where rnk = 1
    -- );
    --drop table ul_end;
    -- create temporary table ul_end as (
    -- select user_id, school_year, created_at act_dt, course_name, script_id, stage_id, level_id from (
    -- 	select ul.user_id, sy.school_year, ul.created_at, cs.course_name_true as course_name, cs.script_id, cs.stage_id, cs.level_id, rank () over (partition by user_id, school_year, course_name_true order by ul.created_at desc, level_script_order desc,level_number desc, ul.script_id desc, cs.stage_id desc ) rnk  -----------------script id ordering
    -- 	from dashboard_production.user_levels ul
    -- 	join cs
    -- 	on ul.script_id = cs.script_id and ul.level_id = cs.level_id
    -- 	join analysis.school_years sy on ul.created_at between sy.started_at and sy.ended_at
    -- 	where ul.attempts > 0
    -- 	)
    -- where rnk = 1
    -- );
    DROP TABLE if EXISTS student_sections_v1;
CREATE temporary TABLE student_sections_v1 AS (
        SELECT
            DISTINCT school_year,
            student_user_id,
            user_id AS teacher_user_id,
            section_id
        FROM
            (
                SELECT
                    sy.school_year,
                    f.student_user_id,
                    s.user_id,
                    s.id section_id,
                    RANK() over (
                        PARTITION BY student_user_id,
                        school_year
                        ORDER BY
                            f.updated_at DESC,
                            s.first_activity_at DESC,
                            s.course_id DESC,
                            s.script_id DESC,
                            s.deleted_at DESC,
                            f.created_at DESC,
                            s.created_at DESC
                    ) rnk ------------- preferred order assumption
                FROM
                    dashboard_production.followers f
                    JOIN dashboard_production.sections s
                    ON f.section_id = s.id -- and first_activity_at <> '1970-01-01 00:00:00'
                    JOIN analysis.school_years sy
                    ON f.created_at BETWEEN sy.started_at
                    AND sy.ended_at
            ) s
        WHERE
            rnk = 1
    );
---------------------------  only assigns one section per user per year
    --  drop table students;
    CREATE temporary TABLE students AS (
        SELECT
            DISTINCT st.user_id || st.school_year || st.course_name AS hs_id,
            st.user_id AS student_user_id,
            st.school_year,
            st.course_name,
            CASE
                WHEN u.races LIKE '%hispanic%' THEN 'Latinx'
                WHEN u.races = 'black' THEN 'Black'
                WHEN u.races = 'asian' THEN 'Asian'
                WHEN u.races = 'american_indian' THEN 'Native American'
                WHEN u.races = 'hawaiian' THEN 'Hawaiian'
                WHEN u.races = 'white' THEN 'white'
                WHEN u.races IN (
                    'closed_dialog',
                    'nonsense',
                    'opt_out'
                ) THEN NULL
                WHEN u.races IS NULL THEN NULL
                WHEN u.urm = 1 THEN 'tr_urg'
                ELSE 'tr_non_urg'
            END AS races,
            CASE
                WHEN u.gender = 'm' THEN 'm'
                WHEN u.gender = 'f' THEN 'f'
                WHEN u.gender = 'n' THEN 'nb' ------  non-binary
                WHEN u.gender = 'o' THEN 'nl' ------  not listed
                ELSE NULL
            END AS gender,
            u.urm,
            DATEDIFF(
                YEAR,
                u.birthday,
                ((LEFT(st.school_year, 2) || RIGHT(st.school_year, 2)) :: INT || '-01-01') :: DATE
            ) AS age,
            ht.school_id,
            ht.school_info_id,
            ht.school_name,
            sts.teacher_user_id,
            ht.teacher_user_id || ht.school_year AS ht_id,
            sts.section_id,
            sts.section_id || st.school_year || st.course_name AS ss_id,
            CASE
                WHEN st.course_name = 'csf'
                AND csf_trained = 1 THEN 1
                WHEN st.course_name = 'csd'
                AND csd_trained = 1 THEN 1
                WHEN st.course_name = 'csp'
                AND csp_trained = 1 THEN 1
                WHEN st.course_name = 'hoc'
                AND csf_trained = 1 THEN 1
                WHEN st.course_name = 'csa'
                AND csa_trained = 1 THEN 1
                ELSE 0
            END AS teacher_trained
        FROM
            analysis.ul_start st
            JOIN analysis.ul_end en
            ON st.user_id = en.user_id
            AND st.school_year = en.school_year
            AND st.course_name = en.course_name
            JOIN dashboard_production_pii.users u
            ON st.user_id = u.id
            AND u.user_type = 'student'
            JOIN dashboard_production.user_geos ug
            ON u.id = ug.user_id
            AND ug.country = 'United States' -------------------------------
            LEFT JOIN student_sections_v1 sts
            ON st.user_id = sts.student_user_id
            AND st.school_year = sts.school_year
            LEFT JOIN PUBLIC.rosetta_historic_teachers ht
            ON ht.teacher_user_id = sts.teacher_user_id
            AND ht.school_year = st.school_year
    );
--drop table public.rosetta_historic_students_wip;
    CREATE TABLE PUBLIC.rosetta_historic_students_wip_BUILD AS (
        SELECT
            *
        FROM
            students
    );
DROP TABLE PUBLIC.rosetta_historic_students_wip;
ALTER TABLE
    PUBLIC.rosetta_historic_students_wip_BUILD RENAME TO rosetta_historic_students_wip;
GRANT
SELECT
    ON PUBLIC.rosetta_historic_students_wip TO GROUP reader_pii;
GRANT ALL
    ON PUBLIC.rosetta_historic_students_wip TO GROUP admin;
-- drop table ul_days;
    -- create temporary table ul_days as (
    -- select ul.user_id, -- rhsw.section_id,
    -- course_name_true as course_name, school_year, to_date(ul.created_at, 'yyyy-mm-dd') dt, count(distinct ul.level_id) all_levels_touched, sum(time_spent) time_spent,
    -- count(distinct (case when l.type in ('CurriculumReference','StandaloneVideo','FreeResponse','External','Map','LevelGroup') then Null --------- UPDATE THIS WHEN LEVEL_TYPES CHANGE!!!!!  NO CSF LEVELS ARE CURRENTLY INCLUDED
    -- else ul.level_id end)) course_progress_levels_touched
    -- from dashboard_production.user_levels ul
    -- join cs on ul.level_id = cs.level_id and ul.script_id = cs.script_id --  limit to decided level types
    -- join dashboard_production.user_geos ug on ul.user_id = ug.user_id and ug.country = 'United States'  --  US only
    -- join dashboard_production_pii.users u on ul.user_id = u.id and u.user_type = 'student' ---- filter to students only
    -- join analysis.school_years sy on ul.created_at between sy.started_at and sy.ended_at
    -- join dashboard_production.levels l on l.id = ul.level_id
    --join public.rosetta_historic_students_wip rhsw on ul.user_id = rhsw.student_user_id and cs.course_name_true = rhsw.course_name and sy.school_year = rhsw.school_year
    --left join section_stats ss on rhsw.section_id = ss.section_id and rhsw.course_name =
    -- where ul.attempts > 0
    -- group by 1,2,3,4
    -- );
    --  NOTE: user_days is basically the same between v1 and v2.  it's annoying to have to recreate it in both places.
    -- The slight difference is calling rosetta historic students versus analysis.students.
    DROP TABLE if EXISTS user_days_v1;
CREATE temporary TABLE user_days_v1 AS (
        SELECT
            ul.user_id,
            ul.school_year,
            rhsw.section_id,
            u.races,
            ul.course_name,
            COUNT(
                DISTINCT dt
            ) unique_days,
            SUM(all_levels_touched) all_levels_touched,
            SUM(course_progress_levels_touched) course_progress_levels_touched,
            SUM(time_spent) time_spent,
            MIN(dt) first_touch,
            MAX(dt) last_touch,
            SUM(
                CASE
                    WHEN EXTRACT(
                        MONTH
                        FROM
                            dt
                    ) = 12
                    AND EXTRACT(
                        DAY
                        FROM
                            dt
                    ) BETWEEN 5
                    AND 15 THEN 1
                    ELSE 0
                END
            ) :: FLOAT / COUNT(
                DISTINCT dt
            ) AS pct_days_in_csed_week --added 4.24.23
        FROM
            analysis.ul_days ul
            JOIN PUBLIC.rosetta_historic_students_wip rhsw
            ON ul.user_id = rhsw.student_user_id
            AND ul.course_name = rhsw.course_name
            AND ul.school_year = rhsw.school_year
            JOIN dashboard_production_pii.users u
            ON u.id = ul.user_id
        GROUP BY
            1,
            2,
            3,
            4,
            5
    );
-- NOTE: user_activity_stats is exactly the same in v2 and should be merged.
    -- However, because of slight differences in user_days between the two I just called this templ table _v1 to avoid
    -- naming conflicts when running rosetta v1 and v2 back to back.  It's annoying to have to recreate this table too.
    DROP TABLE if EXISTS user_activity_stats_v1;
CREATE temporary TABLE user_activity_stats_v1 AS (
        SELECT
            DISTINCT s.student_user_id || s.school_year || s.course_name AS hs_id,
            s.student_user_id,
            s.school_year,
            s.course_name,
            s.section_id,
            s.section_id || s.school_year || s.course_name ss_id,
            ud.unique_days,
            --- Days the user touched any level in the course
            ud.all_levels_touched,
            --------  all levels from the course the user touched
            ud.course_progress_levels_touched,
            -------  all levels of the types defined in ul_days "legit course progress" levels the user completed  (will soon contain logic to limit to section date range)
            ud.time_spent,
            ---------------  The "time spent" field in user_levels
            ud.first_touch,
            ud.last_touch,
            ud.pct_days_in_csed_week,
            st.script_id AS start_script_id,
            st.stage_id AS start_stage_id,
            st.level_id AS start_level_id,
            en.script_id AS end_script_id,
            en.stage_id AS end_stage_id,
            en.level_id AS end_level_id
        FROM
            students s
            JOIN analysis.ul_start st
            ON st.user_id = s.student_user_id
            AND st.school_year = s.school_year
            AND st.course_name = s.course_name
            JOIN analysis.ul_end en
            ON st.user_id = en.user_id
            AND st.school_year = en.school_year
            AND st.course_name = en.course_name
            JOIN user_days_v1 ud
            ON s.student_user_id = ud.user_id
            AND s.course_name = ud.course_name
            AND s.school_year = ud.school_year
            AND COALESCE(
                ud.section_id,
                1
            ) = COALESCE(
                s.section_id,
                1
            )
    );
--drop table public.rosetta_user_activity_stats;
    CREATE TABLE PUBLIC.rosetta_user_activity_stats_BUILD AS (
        SELECT
            *
        FROM
            user_activity_stats_v1
    );
DROP TABLE PUBLIC.rosetta_user_activity_stats;
ALTER TABLE
    PUBLIC.rosetta_user_activity_stats_BUILD RENAME TO rosetta_user_activity_stats;
GRANT
SELECT
    ON PUBLIC.rosetta_user_activity_stats TO GROUP reader_pii;
GRANT ALL
    ON PUBLIC.rosetta_user_activity_stats TO GROUP admin;
-- drop table section_date_range;
    CREATE temporary TABLE section_date_range AS (
        SELECT
            section_id,
            school_year,
            course_name,
            avg_unique_days,
            users,
            avg_levels_touched,
            DATEADD(DAY, first_touch,(LEFT(school_year, 4) || '/07/01') :: DATE) first_touch,
            DATEADD(DAY, last_touch,(LEFT(school_year, 4) || '/07/01') :: DATE) last_touch,
            RANK () over (
                PARTITION BY section_id,
                school_year
                ORDER BY
                    avg_levels_touched DESC,
                    avg_unique_days DESC,
                    last_touch DESC
            ) course_rnk,
            COUNT(course_name) over (
                PARTITION BY section_id,
                school_year
            ) courses_in_section
        FROM
            (
                SELECT
                    section_id,
                    school_year,
                    course_name,
                    AVG(unique_days) avg_unique_days,
                    AVG(
                        DATEDIFF(DAY,(LEFT(school_year, 4) || '/07/01') :: DATE, first_touch)
                    ) first_touch,
                    AVG(
                        DATEDIFF(DAY,(LEFT(school_year, 4) || '/07/01') :: DATE, last_touch)
                    ) last_touch,
                    COUNT(
                        DISTINCT student_user_id
                    ) users,
                    AVG(all_levels_touched) avg_levels_touched
                FROM
                    PUBLIC.rosetta_user_activity_stats
                WHERE
                    all_levels_touched >= 5
                GROUP BY
                    1,
                    2,
                    3
            )
        WHERE
            users >= 5
            AND section_id IS NOT NULL
    );
-- drop table section_stats
    CREATE temporary TABLE section_stats AS (
        SELECT
            section_id || school_year || course_name AS ss_id,
            section_id,
            school_year,
            course_name,
            avg_levels_touched,
            avg_unique_days,
            users,
            first_touch,
            last_touch,
            courses_in_section,
            CASE
                WHEN course_rnk = 1 THEN 1
                ELSE 0
            END AS primary_section_course
        FROM
            section_date_range
    );
--drop table public.rosetta_section_stats;
    CREATE TABLE PUBLIC.rosetta_section_stats_BUILD AS (
        SELECT
            *
        FROM
            section_stats
    );
--future note: get rid of the temp tables.
    DROP TABLE PUBLIC.rosetta_section_stats;
ALTER TABLE
    PUBLIC.rosetta_section_stats_BUILD RENAME TO rosetta_section_stats;
GRANT
SELECT
    ON PUBLIC.rosetta_section_stats TO GROUP reader_pii;
GRANT ALL
    ON PUBLIC.rosetta_section_stats TO GROUP admin;
END;$$
