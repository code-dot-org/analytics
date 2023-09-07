CREATE
OR REPLACE PROCEDURE PUBLIC.run_rosetta_v2() LANGUAGE plpgsql AS $$
BEGIN
    -- create temporary table course_structure as
    -- 	(select * from analysis.course_structure);
    /* 
    the next two tables establish a baseline for student users on the platform
    we use the "first" and "last" user level srecords later on to 
    determine which courses a student "started" 
    */
    --drop table ul_start;
    -- create temporary table ul_start as (
    -- select user_id
    -- 	,school_year
    -- 	,created_at as act_dt --BF: I don't understand this abbr.  act_dt == ?
    -- 	,course_name
    -- 	,script_id
    -- 	,stage_id
    -- 	,level_id
    -- from (
    -- 	select ul.user_id
    -- 		,sy.school_year
    -- 		,ul.created_at
    -- 		,cs.course_name_true as course_name
    -- 		,cs.script_id
    -- 		,cs.stage_id
    -- 		,cs.level_id
    -- 		,rank () over (partition by user_id, school_year, course_name_true order by ul.created_at, level_script_order, level_number, ul.script_id, cs.stage_id) as rnk  -----------------script id ordering
    -- 	from dashboard_production.user_levels ul
    -- 	join course_structure cs ----------------  Inner join because we don't care about levels not in course_structure
    -- 		on ul.script_id = cs.script_id and ul.level_id = cs.level_id
    -- 	join analysis.school_years sy
    -- 		on ul.created_at between sy.started_at and sy.ended_at --------------  In order to select one record per user per course per year we need to attach the activty to a school_year
    -- 	join dashboard_production.users u
    -- 		on u.id = ul.user_id and u.user_type = 'student'
    -- 	where ul.attempts > 0  -- 0 attempts are ul records we want to ignore
    -- 	)
    -- where rnk = 1
    -- );
    --drop table ul_end;
    -- create temporary table ul_end as (
    -- select user_id
    -- 	,school_year
    -- 	,created_at as act_dt
    -- 	,course_name
    -- 	,script_id
    -- 	,stage_id
    -- 	,level_id
    -- from (
    -- 	select ul.user_id
    -- 		,sy.school_year
    -- 		,ul.created_at
    -- 		,cs.course_name_true as course_name
    -- 		,cs.script_id
    -- 		,cs.stage_id
    -- 		,cs.level_id
    -- 		,rank () over (partition by user_id, school_year, course_name_true order by ul.created_at desc, level_script_order desc,level_number desc, ul.script_id desc, cs.stage_id desc ) as rnk  -----------------script id ordering
    -- 	from dashboard_production.user_levels ul
    -- 	join course_structure cs
    -- 		on ul.script_id = cs.script_id and ul.level_id = cs.level_id
    -- 	join analysis.school_years sy
    -- 		on ul.created_at between sy.started_at and sy.ended_at
    -- 	where ul.attempts > 0
    -- 	)
    -- where rnk = 1
    -- );
    --SELECT * FROM ul_end LIMIT 100;
    /*
    student_sections establishes each section that a user was a part of
    during a given school year based on the date of assignment of the
    follower record.  We do create a rank based on recency for students
    in multiple sections but we don't use it as a filter until later.
    */
    DROP TABLE if EXISTS student_sections_v2;
CREATE temporary TABLE student_sections_v2 AS (
        SELECT
            DISTINCT school_year,
            student_user_id,
            user_id AS teacher_user_id,
            section_id,
            grade,
            age_at_start -- is the students age at the start of the school year,
            rnk
        FROM
            (
                SELECT
                    sy.school_year,
                    f.student_user_id,
                    s.user_id,
                    s.id section_id,
                    s.grade,
                    DATEDIFF(
                        YEAR,
                        u.birthday,
                        sy.started_at
                    ) AS age_at_start,
                    RANK() over (
                        PARTITION BY student_user_id,
                        school_year
                        ORDER BY
                            f.created_at DESC,
                            s.first_activity_at DESC,
                            s.course_id DESC,
                            s.script_id DESC,
                            s.deleted_at DESC,
                            f.updated_at DESC,
                            s.created_at DESC
                    ) AS rnk ------------- preferred order assumption
                FROM
                    dashboard_production.followers f
                    JOIN dashboard_production.sections s
                    ON f.section_id = s.id
                    JOIN analysis.school_years sy
                    ON f.created_at BETWEEN sy.started_at
                    AND sy.ended_at
                    JOIN dashboard_production_pii.users u
                    ON f.student_user_id = u.id
            ) s
    );
-- SELECT COUNT(*) FROM followers LIMIT 1000;
    -- SELECT * FROM sections LIMIT 1000;
    -- SELECT COUNT(*) FROM users LIMIT 1000;
    --
    -- SELECT * FROM student_sections_v2 LIMIT 100;
    /*
    This table answers the question, "did more than five students in 
    a given section start a course?"  Typically, we think of "starts"
    in terms of users, but with the shift towards counting schools,
    whether or not a specific "classroom (section)" started using
    a product is an important outcome to keep track of.
    */
    DROP TABLE if EXISTS section_starts_tmp;
CREATE temporary TABLE section_starts_tmp AS (
        SELECT
            DISTINCT teacher_user_id,
            school_year,
            course_name,
            section_id,
            started_at -- baker: added this to the section_start record
        FROM
            (
                SELECT
                    ss.teacher_user_id,
                    ss.school_year,
                    s.course_name,
                    ss.section_id,
                    COUNT(
                        DISTINCT student_user_id
                    ) students -----------Baker adding this--------,
                    MIN(
                        s.act_dt
                    ) started_at -- a section's started_date is the first ul.created_at timestamp for any student-user_level record for this section/course
                    ------------------------------------
                FROM
                    analysis.ul_start s
                    JOIN student_sections_v2 ss
                    ON s.user_id = ss.student_user_id
                    AND s.school_year = ss.school_year
                GROUP BY
                    1,
                    2,
                    3,
                    4
            )
        WHERE
            students >= 5
    );
INSERT INTO
    analysis.health_check (
        SELECT
            'analysis.section_starts',
            old_count,
            new_count,
            new_count - old_count,
            CURRENT_DATE
        FROM
            (
                SELECT
                    COUNT(*) new_count
                FROM
                    section_starts_tmp
            ) nw
            JOIN (
                SELECT
                    COUNT(*) old_count
                FROM
                    analysis.section_starts
            ) od
            ON 1 = 1
    );
GRANT ALL
    ON analysis.health_check TO GROUP admin;
GRANT
SELECT
    ON analysis.section_starts TO GROUP reader_pii;
    /*
    Now that everything is updated in the temp table, we overwrite the previously
    stored values.
    */
    DROP TABLE if EXISTS analysis.section_starts;
CREATE TABLE analysis.section_starts AS (
        SELECT
            *
        FROM
            section_starts_tmp
    );
GRANT ALL
    ON analysis.section_starts TO GROUP admin;
GRANT
SELECT
    ON analysis.section_starts TO GROUP reader_pii;
    /*
    Because what we care about in teacher starts is whether or not a classroom
    they were leading started a course, we can use the section_starts table
    as a basis for teacher starts.  Because we did not limit students to one 
    section, we DO attribute student activity to more than one teacher, if the 
    student is in two sections with different teachers.  Code to create exclusive
    logic is stored below the full code.
    */
    DROP TABLE if EXISTS teacher_starts_tmp;
CREATE temporary TABLE teacher_starts_tmp AS (
        SELECT
            DISTINCT teacher_user_id,
            school_year,
            course_name -------Baker adding this-----,
            MIN(started_at) started_at -- a teacher's started_at is the first started_at for any of their sections
            -----------------------------
        FROM
            analysis.section_starts
        GROUP BY
            1,
            2,
            3
    );
-----------------------------  Pending decision, switch this in for the above
    /*
    create temporary table teacher_starts_one_student_one_teacher as (
    select distinct teacher_user_id
        ,school_year
        ,course_name 
    from
        (
        select ss.teacher_user_id
            ,ss.school_year
            ,s.course_name
            ,ss.section_id
            ,count(distinct student_user_id) students 
        from ul_start s
        join student_sections_v2 ss 
        on s.user_id = ss.student_user_id and s.school_year = ss.school_year and rnk = 1
        group by 1,2,3,4
        )
    where students >= 5
    );
    */
    /*
    For each teacher, we select whether or not a teacher has been trained for any of our courses
    AND what year that PD  took place
    */
    --drop table teacher_trainings;
    CREATE temporary TABLE teacher_trainings AS (
        SELECT
            'csf' AS course_name,
            user_id,
            school_year
        FROM
            analysis.csf_teachers_trained tt
            JOIN analysis.school_years sy
            ON tt.trained_at BETWEEN ADD_MONTHS(
                sy.started_at,
                -2
            )
            AND ADD_MONTHS(
                sy.ended_at,
                -2
            )
        UNION
        SELECT
            'csd' AS course_name,
            u.id AS user_id,
            tt.school_year
        FROM
            analysis.csp_csd_teachers_trained tt
            LEFT JOIN dashboard_production_pii.users u
            ON tt.studio_person_id = u.studio_person_id
        WHERE
            tt.course = 'CS Discoveries'
        UNION
        SELECT
            'csp' AS course_name,
            u.id AS user_id,
            tt.school_year
        FROM
            analysis.csp_csd_teachers_trained tt
            LEFT JOIN dashboard_production_pii.users u
            ON tt.studio_person_id = u.studio_person_id
        WHERE
            tt.course = 'CS Principles'
        UNION
        SELECT
            'csa' AS course_name,
            u.id AS user_id,
            tt.school_year
        FROM
            analysis.csp_csd_teachers_trained tt
            LEFT JOIN dashboard_production_pii.users u
            ON tt.studio_person_id = u.studio_person_id
        WHERE
            tt.course = 'Computer Science A'
    );
    /*
     Assigns one or more school record for each teacher if they have a user school infos 
     gateway and assigns start and end dates and US Teachers only
    */
    --drop table if exists teacher_schools;
    CREATE temporary TABLE teacher_schools AS (
        SELECT
            DISTINCT u.id user_id,
            usi.school_info_id,
            si.school_id,
            COALESCE(
                si.school_name,
                s.name
            ) AS school_name -------------  School infos table is more accurate than schools,
            usi.start_date rec_eff_dt,
            CASE
                WHEN usi.end_date IS NOT NULL THEN end_date
                ELSE '9999-12-31'
            END AS rec_cncl_dt --, si.country si_country --baker add,
            ug.country ug_country --baker add,
            si.state si_state --baker add,
            COALESCE(
                --BF: I might invert this coalesce.  It appears that lots of people claim US as a country on school_infos, that doesn't appear to be true.  We should prioritized school_stats country
                CASE
                    WHEN s.id IS NOT NULL THEN 'US' --if it has a proper school_id then it's US
                    WHEN si.state IN (
                        'CA',
                        'AZ',
                        'FL',
                        'TX',
                        'GA',
                        'WA',
                        'NV',
                        'SD',
                        'WI',
                        'HI',
                        'NY',
                        'PA',
                        'RI',
                        'VA',
                        'NC',
                        'OR',
                        'WY',
                        'IL',
                        'MO',
                        'OH',
                        'AL',
                        'UT',
                        'MA',
                        'KS',
                        'ND',
                        'NM',
                        'MS',
                        'IN',
                        'KY',
                        'MI',
                        'CT',
                        'MD',
                        'ID',
                        'IA',
                        'OK',
                        'MN',
                        'TN',
                        'LA',
                        'ME',
                        'CO',
                        'DE',
                        'Maryland',
                        'AR',
                        'NJ',
                        'MT',
                        'Washington',
                        'Indiana',
                        'Tennessee',
                        'SC',
                        'Mississippi',
                        'Texas',
                        'California',
                        'Iowa'
                    ) THEN 'US'
                    ELSE NULL
                END,
                si.country
            ) AS si_country,
            usi.last_confirmation_date
        FROM
            dashboard_production.users u
            LEFT JOIN dashboard_production_pii.user_school_infos usi
            ON u.id = usi.user_id
            LEFT JOIN dashboard_production.school_infos si
            ON si.id = usi.school_info_id
            LEFT JOIN dashboard_production.schools s
            ON si.school_id = s.id
            JOIN dashboard_production.user_geos ug
            ON u.id = ug.user_id --and ug.country = 'United States'
        WHERE
            u.user_type = 'teacher'
            AND (
                si_country = 'US'
                OR ug.country = 'United States'
            )
    );
-- SELECT count(distinct(id)), country, state
    -- FROM school_infos
    -- WHERE country IN ('US','United States') OR (country IS NULL and state IS NOT NULL)
    -- GROUP BY 2,3 LIMIT 1000;
    --
    -- SELECT * FROM school_infos WHERE school_id IS NOT NULL LIMIT 1000;
    --
    -- SELECT * FROM users LIMIT 1000;
    --
    --
    -- SELECT * FROM user_geos ORDER BY user_id LIMIT 1000;
    --
    -- SELECT * FROM user_school_infos  LIMIT 1000;
    -- drop table teacher_years;
    CREATE temporary TABLE teacher_years AS (
        SELECT
            u.id AS user_id,
            school_year
        FROM
            dashboard_production_pii.users u
            JOIN analysis.school_years sy
            ON u.created_at < sy.ended_at
            AND sy.started_at < CURRENT_DATE
        WHERE
            u.user_type = 'teacher'
    );
    /*
    Limits each teacher to one school per school year with a preference for a valid NCES ID and recency for US Teachers
    */
    -- drop table teacher_sy_assignments;
    CREATE temporary TABLE teacher_sy_assignments AS (
        SELECT
            DISTINCT user_id,
            school_info_id,
            school_id,
            school_name,
            rec_eff_dt,
            rec_cncl_dt,
            school_year,
            si_country
        FROM
            (
                SELECT
                    ts.*,
                    sy.school_year,
                    RANK () over (
                        PARTITION BY ts.user_id,
                        sy.school_year
                        ORDER BY
                            ts.last_confirmation_date DESC,
                            ts.rec_cncl_dt DESC
                    ) rnk
                FROM
                    teacher_schools ts
                    JOIN analysis.school_years sy
                    ON ts.rec_eff_dt < sy.ended_at
                    AND ts.rec_cncl_dt > sy.started_at
                    AND sy.started_at < CURRENT_DATE
            )
        WHERE
            rnk = 1
    );
    /*
    Combines teacher school records with years since the teacher record was 
    created and adds a unique row for every course per school year for all US Teachers
    */
    -- drop table teacher_info;
    CREATE temporary TABLE teacher_info AS (
        SELECT
            DISTINCT ty.*,
            school_info_id,
            school_id,
            school_name,
            course_name_true AS course_name
        FROM
            teacher_years ty
            JOIN teacher_sy_assignments tsa
            ON ty.user_id = tsa.user_id
            AND ty.school_year = tsa.school_year
            LEFT JOIN (
                SELECT
                    DISTINCT course_name_true
                FROM
                    analysis.course_structure
            ) cs
            ON 1 = 1
        WHERE
            tsa.si_country IN (
                'US',
                'United States'
            )
    );
    /*
    Joins starts and trainings onto the main record and limits 
    to teachers who have either training or platform activty for US Teachers
    */
    -- drop table rht;
    CREATE temporary TABLE rht AS (
        SELECT
            ti.*,CASE
                WHEN ts.teacher_user_id IS NOT NULL THEN 1
                ELSE 0
            END AS started ---------baker adding this --------------,CASE
                WHEN ts.teacher_user_id IS NOT NULL THEN ts.started_at
                ELSE NULL
            END AS started_at -----------------------------------------,CASE
                WHEN tt.school_year <= ti.school_year THEN 1
                ELSE 0
            END AS trained,CASE
                WHEN yt.user_id IS NOT NULL THEN 1
                ELSE 0
            END AS trained_this_year
        FROM
            teacher_info ti
            LEFT JOIN teacher_starts_tmp ts
            ON ti.user_id = ts.teacher_user_id
            AND ti.school_year = ts.school_year
            AND ti.course_name = ts.course_name
            LEFT JOIN (
                SELECT
                    course_name,
                    user_id,
                    MIN(school_year) school_year
                FROM
                    teacher_trainings
                GROUP BY
                    1,
                    2
            ) tt
            ON ti.user_id = tt.user_id
            AND ti.course_name = tt.course_name
            LEFT JOIN teacher_trainings yt
            ON ti.user_id = yt.user_id
            AND ti.school_year = yt.school_year
            AND ti.course_name = yt.course_name
            LEFT JOIN (
                SELECT
                    DISTINCT course_name,
                    teacher_user_id user_id
                FROM
                    teacher_starts_tmp
            ) es
            ON ti.user_id = es.user_id
            AND ti.course_name = es.course_name
        WHERE
            (
                tt.user_id IS NOT NULL
                OR es.user_id IS NOT NULL
            )
    );
INSERT INTO
    analysis.health_check (
        SELECT
            'analysis.teachers',
            old_count,
            new_count,
            new_count - old_count,
            CURRENT_DATE
        FROM
            (
                SELECT
                    COUNT(*) new_count
                FROM
                    rht
            ) nw
            JOIN (
                SELECT
                    COUNT(*) old_count
                FROM
                    analysis.teachers
            ) od
            ON 1 = 1
    );
DROP TABLE analysis.teachers;
CREATE TABLE analysis.teachers AS (
        SELECT
            *
        FROM
            rht
    );
GRANT ALL
    ON analysis.teachers TO GROUP admin;
GRANT
SELECT
    ON analysis.teachers TO GROUP reader_pii;
--SELECT count(*), school_year FROM analysis.teachers WHERE trained_this_year=1 and course_name = 'csa' GROUP BY 2 LIMIT 100;
    /* the next two tables can be a little confusing:
    * analysis.students only contains one record per student, course, and school year dispite the fact that students
    	might be in more than one section per school year.  This is a student centric table that should be used for most
    	queries related to counting students.
    * analysis.student_teacher_section_complete is a complete mapping of students to sections and teachers.  In an earlier note
    	it was mentioned that student activity can count towards multiple sections when determining section starts.  When
    	we limit students to one section in the above query, we lose a full accounting of "how" a section started.  This
    	table is an attempt to make explicit this difference.
    	
    The key difference between these two tables is highlighted below.
    
    */
    CREATE temporary TABLE rhs AS (
        SELECT
            DISTINCT st.user_id || st.school_year || st.course_name || sts.section_id AS hs_id,
            st.user_id AS student_user_id,
            st.school_year,
            st.course_name,CASE
                WHEN u.races LIKE '%hispanic%' THEN 'Hispanic'
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
            ht.user_id || ht.school_year || ht.course_name AS ht_id,
            sts.section_id,
            sts.section_id || st.school_year || st.course_name AS ss_id,
            CASE
                WHEN ht.trained = 1 THEN 1
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
            LEFT JOIN student_sections_v2 sts
            ON st.user_id = sts.student_user_id
            AND st.school_year = sts.school_year
            AND sts.rnk = 1 --<<<<<<<<<<<< This rank filter limits each student to one section per school year
            LEFT JOIN analysis.teachers ht
            ON ht.user_id = sts.teacher_user_id
            AND ht.school_year = st.school_year
            AND ht.course_name = st.course_name
    );
INSERT INTO
    analysis.health_check (
        SELECT
            'analysis.students',
            old_count,
            new_count,
            new_count - old_count,
            CURRENT_DATE
        FROM
            (
                SELECT
                    COUNT(*) new_count
                FROM
                    rhs
            ) nw
            JOIN (
                SELECT
                    COUNT(*) old_count
                FROM
                    analysis.students
            ) od
            ON 1 = 1
    );
DROP TABLE analysis.students;
CREATE TABLE analysis.students AS (
        SELECT
            *
        FROM
            rhs
    );
GRANT ALL
    ON analysis.students TO GROUP admin;
GRANT
SELECT
    ON analysis.students TO GROUP reader_pii;
CREATE temporary TABLE fa AS (
        SELECT
            DISTINCT st.user_id || st.school_year || st.course_name || sts.section_id AS hs_id,
            st.user_id AS student_user_id,
            st.school_year,
            st.course_name,CASE
                WHEN u.races LIKE '%hispanic%' THEN 'Hispanic'
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
            ht.user_id || ht.school_year || ht.course_name AS ht_id,
            sts.section_id,
            sts.section_id || st.school_year || st.course_name AS ss_id,
            CASE
                WHEN ht.trained = 1 THEN 1
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
            LEFT JOIN student_sections_v2 sts
            ON st.user_id = sts.student_user_id
            AND st.school_year = sts.school_year
            LEFT JOIN analysis.teachers ht
            ON ht.user_id = sts.teacher_user_id
            AND ht.school_year = st.school_year
            AND ht.course_name = st.course_name
    );
INSERT INTO
    analysis.health_check (
        SELECT
            'analysis.student_teacher_section_complete',
            old_count,
            new_count,
            new_count - old_count,
            CURRENT_DATE
        FROM
            (
                SELECT
                    COUNT(*) new_count
                FROM
                    fa
            ) nw
            JOIN (
                SELECT
                    COUNT(*) old_count
                FROM
                    analysis.student_teacher_section_complete
            ) od
            ON 1 = 1
    );
DROP TABLE analysis.student_teacher_section_complete;
CREATE TABLE analysis.student_teacher_section_complete AS (
        SELECT
            *
        FROM
            fa
    );
GRANT ALL
    ON analysis.student_teacher_section_complete TO GROUP admin;
GRANT
SELECT
    ON analysis.student_teacher_section_complete TO GROUP reader_pii;
DROP TABLE if EXISTS user_days_v2;
CREATE temporary TABLE user_days_v2 AS (
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
            JOIN analysis.students rhsw
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
    /*
    User activity stats is supposed to be a source for student activity on the platform in an aggregate and
    high level summarization.
    */
    -- drop table user_activity_stats;
    CREATE temporary TABLE user_activity_stats_v2 AS (
        SELECT
            DISTINCT s.student_user_id || s.school_year || s.course_name AS hs_id,
            s.student_user_id,
            s.school_year,
            s.course_name,
            s.section_id,
            s.section_id || s.school_year || s.course_name ss_id,
            ud.unique_days --- Days the user touched any level in the course,
            ud.all_levels_touched --------  all levels from the course the user touched,
            ud.course_progress_levels_touched -------  all levels of the types defined in ul_days "legit course progress" levels the user completed  (will soon contain logic to limit to section date range),
            ud.time_spent ---------------  The "time spent" field in user_levels,
            ud.first_touch,
            ud.last_touch,
            pct_days_in_csed_week --added 4.24.23,
            st.script_id AS start_script_id,
            st.stage_id AS start_stage_id,
            st.level_id AS start_level_id,
            en.script_id AS end_script_id,
            en.stage_id AS end_stage_id,
            en.level_id AS end_level_id
        FROM
            analysis.students s
            JOIN analysis.ul_start st
            ON st.user_id = s.student_user_id
            AND st.school_year = s.school_year
            AND st.course_name = s.course_name
            JOIN analysis.ul_end en
            ON st.user_id = en.user_id
            AND st.school_year = en.school_year
            AND st.course_name = en.course_name
            JOIN user_days_v2 ud
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
INSERT INTO
    analysis.health_check (
        SELECT
            'analysis.student_activity_stats',
            old_count,
            new_count,
            new_count - old_count,
            CURRENT_DATE
        FROM
            (
                SELECT
                    COUNT(*) new_count
                FROM
                    user_activity_stats_v2
            ) nw
            JOIN (
                SELECT
                    COUNT(*) old_count
                FROM
                    analysis.student_activity_stats
            ) od
            ON 1 = 1
    );
DROP TABLE analysis.student_activity_stats;
CREATE TABLE analysis.student_activity_stats AS (
        SELECT
            *
        FROM
            user_activity_stats_v2
    );
GRANT ALL
    ON analysis.student_activity_stats TO GROUP admin;
GRANT
SELECT
    ON analysis.student_activity_stats TO GROUP reader_pii;
    /* 
    Because median in redshift if a window function and cannot be used
    in conjunction with other aggregate functions, we create a table 
    with the median age for each section in each school year (it should not be different 
    per course, and if it is, we probably want the overall median)
    */
    CREATE temporary TABLE section_age AS (
        SELECT
            ss.school_year,
            ss.section_id,
            MEDIAN(
                ss.age_at_start
            ) sy_age
        FROM
            student_sections_v2 ss
            JOIN (
                SELECT
                    DISTINCT user_id,
                    school_year
                FROM
                    analysis.ul_start
            ) st
            ON ss.student_user_id = st.user_id
            AND ss.school_year = st.school_year
        GROUP BY
            1,
            2
    );
--drop table section_grade;
    CREATE temporary TABLE section_grade AS (
        SELECT
            DISTINCT teacher_user_id,
            school_year,
            section_id,
            grade,CASE
                WHEN grade IN (
                    'K',
                    '1',
                    '2',
                    '3',
                    '4',
                    '5'
                ) THEN 'El'
                WHEN grade IN (
                    '6',
                    '7',
                    '8'
                ) THEN 'Mi'
                WHEN grade IN (
                    '9',
                    '10',
                    '11',
                    '12'
                ) THEN 'Hi'
                ELSE NULL
            END AS grade_based,
            sy_age,CASE
                WHEN sy_age >= 14 THEN 'Hi'
                WHEN sy_age BETWEEN 10.5
                AND 13.5 THEN 'Mi'
                WHEN sy_age <= 10 THEN 'El'
                ELSE NULL
            END age_based
        FROM
            (
                SELECT
                    ss.teacher_user_id,
                    ss.school_year,
                    ss.section_id,
                    COUNT(
                        DISTINCT student_user_id
                    ) students,
                    MAX(grade) grade,
                    MAX(sy_age) :: FLOAT sy_age
                FROM
                    student_sections_v2 ss
                    JOIN section_age sa
                    ON ss.section_id = sa.section_id
                    AND ss.school_year = sa.school_year
                GROUP BY
                    1,
                    2,
                    3
            )
        WHERE
            students >= 5
    );
    /*
    This table establishes some baselines and descriptive stats about what is happening
    in the sections.
    */
    DROP TABLE if EXISTS section_date_range_v2;
CREATE temporary TABLE section_date_range_v2 AS (
        SELECT
            section_id,
            school_year,
            course_name,
            avg_unique_days,
            users,
            avg_levels_touched,
            DATEADD(DAY, first_touch,(LEFT(school_year, 4) || '/07/01') :: DATE) first_touch,
            DATEADD(DAY, last_touch,(LEFT(school_year, 4) || '/07/01') :: DATE) last_touch,
            avg_pct_days_in_csed_week,
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
                    AVG(
                        pct_days_in_csed_week :: FLOAT
                    ) avg_pct_days_in_csed_week -- added 04.24.23 -- this is kind of an average of averages, but ok in this instance since we want to know across the section the avg. number of days spent in CS Ed week eqaully for each user.,
                    COUNT(
                        DISTINCT student_user_id
                    ) users,
                    AVG(all_levels_touched) avg_levels_touched
                FROM
                    PUBLIC.rosetta_user_activity_stats --this is a reference to a table in rosetta_v1
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
    /* 
    Assigns a primary course that a section is teaching, the logic for the rank 
    statement is in the table above
    */
    -- drop table section_stats_v2;
    CREATE temporary TABLE section_stats_v2 AS (
        SELECT
            sdr.section_id || sdr.school_year || sdr.course_name AS ss_id,
            sdr.section_id,
            sdr.school_year,
            sdr.course_name,
            sdr.avg_levels_touched,
            sdr.avg_unique_days,
            sdr.users,
            sdr.first_touch,
            sdr.last_touch,
            sdr.courses_in_section,CASE
                WHEN course_rnk = 1 THEN 1
                ELSE 0
            END AS primary_section_course,CASE
                WHEN ss.section_id IS NOT NULL THEN 1
                ELSE 0
            END AS section_start,
            r.school_id,
            ss.teacher_user_id,
            sdr.avg_pct_days_in_csed_week
        FROM
            section_date_range_v2 sdr
            LEFT JOIN analysis.section_starts ss
            ON sdr.section_id = ss.section_id
            AND sdr.school_year = ss.school_year
            AND sdr.course_name = ss.course_name --left join rht r on ss.teacher_user_id = r.user_id and ss.school_year = r.school_year and ss.course_name = r.course_name and r.started = 1
            LEFT JOIN analysis.teachers r
            ON ss.teacher_user_id = r.user_id
            AND ss.school_year = r.school_year
            AND ss.course_name = r.course_name
            AND r.started = 1 -- changed left join on 4-26-23.  Analysis.teachers is synonymous wth the rht temp table.
    );
INSERT INTO
    analysis.health_check (
        SELECT
            'analysis.section_stats',
            old_count,
            new_count,
            new_count - old_count,
            CURRENT_DATE
        FROM
            (
                SELECT
                    COUNT(*) new_count
                FROM
                    section_stats_v2
            ) nw
            JOIN (
                SELECT
                    COUNT(*) old_count
                FROM
                    analysis.section_stats
            ) od
            ON 1 = 1
    );
---allisons code to find diffs
    -- SELECT *
    -- FROM section_stats ss
    -- LEFT JOIN rht t --analysis.teachers t
    -- ON ss.teacher_user_id = t.user_id
    -- and ss.course_name = t.course_name
    -- and ss.school_year = t.school_year
    -- WHERE ss.course_name='csa'
    -- and ss.school_year IN ('2022-23')
    -- and t.user_id is null;
    --This is now down to 4 missing teachers ...which may be non-US teachers and should be excluded.
    --quick check: how many more teachers are now included in rht?
    -- SELECT COUNT(*), count(distinct(user_id)), count(distinct(school_id)) FROM rht;
    -- SELECT COUNT(*), count(distinct(user_id)), count(distinct(school_id)) FROM analysis.teachers;
    DROP TABLE if EXISTS analysis.section_stats;
CREATE TABLE analysis.section_stats AS (
        SELECT
            *
        FROM
            section_stats_v2
    );
GRANT ALL
    ON analysis.section_stats TO GROUP admin;
GRANT
SELECT
    ON analysis.section_stats TO GROUP reader_pii;
    /*
    This table summarizes the grade or school type that a section is teaching.
    This logic is the basis for much of the aggregation logic in goals.
    */
    CREATE temporary TABLE sg AS (
        SELECT
            DISTINCT teacher_user_id,
            section_id,
            sg.school_year,
            COALESCE(
                grade_based,
                age_based
            ) AS school_type,
            grade,
            sy_age AS avg_age,
            grade_based,
            age_based,
            school_id,
            school_info_id
        FROM
            section_grade sg
            LEFT JOIN teacher_sy_assignments ts
            ON sg.teacher_user_id = ts.user_id
            AND sg.school_year = ts.school_year
        WHERE
            school_type IS NOT NULL
    );
INSERT INTO
    analysis.health_check (
        SELECT
            'analysis.section_grade',
            old_count,
            new_count,
            new_count - old_count,
            CURRENT_DATE
        FROM
            (
                SELECT
                    COUNT(*) new_count
                FROM
                    sg
            ) nw
            JOIN (
                SELECT
                    COUNT(*) old_count
                FROM
                    analysis.section_grade
            ) od
            ON 1 = 1
    );
DROP TABLE analysis.section_grade;
CREATE TABLE analysis.section_grade AS (
        SELECT
            *
        FROM
            sg
    );
GRANT ALL
    ON analysis.section_grade TO GROUP admin;
GRANT
SELECT
    ON analysis.section_grade TO GROUP reader_pii;
END;$$
