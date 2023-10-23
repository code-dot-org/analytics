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