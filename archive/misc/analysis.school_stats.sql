CREATE
OR REPLACE VIEW analysis.school_stats as
/*  This code has been updated to account for an FRL anomaly during school year 
 2020-2021 that was caused by the pandemic.  The extra join and the the COALESCE
 statements contained in the FRL code are added to ignore the most recent year 
 if that year is 2020-21 and instead use last year's FRL data for the school.
 The old view code for analysis.school_stats from prior to 7/13/22 is stored as 
 school_stats.sql in the SQL snippets folder of the RED team drive.
 */
SELECT
    schools.id AS school_id,
    schools.name AS school_name,
    schools.city AS city,
    schools.zip AS zip,
    schools.state AS state,
    schools.latitude AS latitude,
    schools.longitude AS longitude,
    schools.school_type AS school_type,
    schools.school_district_id AS school_district_id,
    school_districts.name AS school_district_name,
    survey_years.survey_year AS survey_year,
    survey_years.first_survey_year AS first_survey_year,
    ssby.grades_offered_lo AS grades_lo,
    ssby.grades_offered_hi AS grades_hi,
    (
        CASE
        WHEN ssby.grades_offered_lo is null then null
        WHEN (
            ssby.grade_pk_offered + ssby.grade_kg_offered + ssby.grade_01_offered + ssby.grade_02_offered + ssby.grade_03_offered + ssby.grade_04_offered + ssby.grade_05_offered
        ) > 0 THEN 1
        WHEN ssby.grades_offered_lo in ('01', '02', '03', '04', '05', 'PK', 'KG') THEN 1
        ELSE 0 END
    ) AS stage_el,
    (
        CASE
        WHEN ssby.grades_offered_lo is null then null
        WHEN -- exclude K-6 and pre-K-6 schools from being classified as middle schools
        (
            (
                ssby.grades_offered_lo = 'PK'
                and ssby.grades_offered_hi = '06'
            )
            or (
                ssby.grades_offered_lo = 'KG'
                and ssby.grades_offered_hi = '06'
            )
        ) = 1 THEN 0
        WHEN (
            ssby.grade_06_offered + ssby.grade_07_offered + ssby.grade_08_offered
        ) > 0 THEN 1
        WHEN ssby.grades_offered_lo in ('06', '07', '08')
        OR ssby.grades_offered_hi in ('06', '07', '08') THEN 1
        ELSE 0 END
    ) AS stage_mi,
    (
        CASE
        WHEN ssby.grades_offered_lo is null then null
        WHEN (
            ssby.grade_09_offered + ssby.grade_10_offered + ssby.grade_11_offered + ssby.grade_12_offered + ssby.grade_13_offered
        ) > 0 THEN 1
        WHEN ssby.grades_offered_hi in ('09', '10', '11', '12') THEN 1
        ELSE 0 END
    ) AS stage_hi,
    ssby.students_total AS students,
    ssby.student_am_count AS student_am,
    ssby.student_as_count AS student_as,
    ssby.student_hi_count AS student_hi,
    ssby.student_bl_count AS student_bl,
    ssby.student_wh_count AS student_wh,
    ssby.student_hp_count AS student_hp,
    ssby.student_tr_count AS student_tr,
    coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_as_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_wh_count, 0) + coalesce(ssby.student_hp_count, 0) + coalesce(ssby.student_tr_count, 0) as students_summed,
    ---------------  sh to allow for manipulation later
    case
    when ssby.students_total = coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_as_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_wh_count, 0) + coalesce(ssby.student_hp_count, 0) + coalesce(ssby.student_tr_count, 0) then (
        coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_hp_count, 0)
    ) / ssby.students_total:: float END AS urm_percent,
    case
    when.7 <= (
        coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_as_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_wh_count, 0) + coalesce(ssby.student_hp_count, 0) + coalesce(ssby.student_tr_count, 0)
    ):: float / ssby.students_total:: float then (
        coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_hp_count, 0)
    ) / (
        coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_as_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_wh_count, 0) + coalesce(ssby.student_hp_count, 0) + coalesce(ssby.student_tr_count, 0)
    ):: float END AS urm_percent_true,
    ----------------  (sh) adds all schools where sum <> total reported students, this allows us to extrapolate percentages at the school
    case
    when 0 < coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_as_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_wh_count, 0) + coalesce(ssby.student_hp_count, 0) then (
        coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_hp_count, 0)
    ) / (
        coalesce(ssby.student_am_count, 0) + coalesce(ssby.student_as_count, 0) + coalesce(ssby.student_hi_count, 0) + coalesce(ssby.student_bl_count, 0) + coalesce(ssby.student_wh_count, 0) + coalesce(ssby.student_hp_count, 0)
    ):: float END AS urm_percent_no_tr,
    /* this next section is where we made the edits to support the new FRL stop-gap logic*/
    case
    when survey_years.survey_year = '2020-2021' then coalesce(ssby2.frl_eligible_total, ssby.frl_eligible_total) ---- coalesce statement added to next three fields to account for covid FRL effects
    else ssby.frl_eligible_total end AS frl_eligible,
    case
    when survey_years.survey_year = '2020-2021' then coalesce(
        (
            CASE
            WHEN ssby2.frl_eligible_total IS NULL
            OR ssby2.students_total IS NULL
            OR ssby2.frl_eligible_total > ssby2.students_total THEN NULL
            ELSE ssby2.frl_eligible_total / ssby2.students_total:: float END
        ),
        (
            CASE
            WHEN ssby.frl_eligible_total IS NULL
            OR ssby.students_total IS NULL
            OR ssby.frl_eligible_total > ssby.students_total THEN NULL
            ELSE ssby.frl_eligible_total / ssby.students_total:: float END
        )
    )
    else (
        CASE
        WHEN ssby.frl_eligible_total IS NULL
        OR ssby.students_total IS NULL
        OR ssby.frl_eligible_total > ssby.students_total THEN NULL
        ELSE ssby.frl_eligible_total / ssby.students_total:: float END
    ) end AS frl_eligible_percent,
    case
    when survey_years.survey_year = '2020-2021' then coalesce(
        (
            CASE
            WHEN ssby2.frl_eligible_total IS NULL
            OR ssby2.students_total IS NULL THEN NULL
            WHEN (
                ssby2.frl_eligible_total / ssby2.students_total:: float
            ) > 0.5 THEN 1
            ELSE 0 END
        ),
        (
            CASE
            WHEN ssby.frl_eligible_total IS NULL
            OR ssby.students_total IS NULL THEN NULL
            WHEN (
                ssby.frl_eligible_total / ssby.students_total:: float
            ) > 0.5 THEN 1
            ELSE 0 END
        )
    )
    else (
        CASE
        WHEN ssby.frl_eligible_total IS NULL
        OR ssby.students_total IS NULL THEN NULL
        WHEN (
            ssby.frl_eligible_total / ssby.students_total:: float
        ) > 0.5 THEN 1
        ELSE 0 END
    ) end AS high_needs,
    -- end altered code
    case
    when ssby.title_i_status in ('1', '2', '3', '4', '5') then 1
    when ssby.title_i_status = '6' then 0
    else null end as title_i,
    ssby.community_type AS community_type,
    case
    when ssby.community_type in (
        'rural_fringe',
        'rural_distant',
        'rural_remote',
        'town_remote',
        'town_distant'
    ) then 1
    when ssby.community_type is not null then 0 end as rural
FROM
    dashboard_production.schools
    LEFT JOIN dashboard_production.school_districts ON schools.school_district_id = school_districts.id
    LEFT JOIN (
        SELECT
            MAX(school_year) AS survey_year,
            MIN(school_year) AS first_survey_year,
            school_id
        FROM
            dashboard_production.school_stats_by_years
        GROUP BY
            school_id
    ) survey_years ON survey_years.school_id = schools.id
    LEFT JOIN dashboard_production.school_stats_by_years ssby ON ssby.school_id = schools.id
    AND ssby.school_year = survey_years.survey_year
    LEFT JOIN dashboard_production.school_stats_by_years ssby2 -----  this join is new to facilitate the FRL stop gap logic.
    ON ssby2.school_id = schools.id
    AND ssby2.school_year = '2019-2020' WITH NO SCHEMA BINDING;