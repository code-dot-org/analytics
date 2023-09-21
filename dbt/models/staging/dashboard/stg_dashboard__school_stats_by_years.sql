with

school_stats_by_years as (
    select
        *,

        case
            when title_i_status in (1, 2, 3, 4, 5)
                then 1
            when title_i_status = 6
                then 0
        end as is_title_i,

        case
            when
                community_type in (
                    'rural_fringe',
                    'rural_distant',
                    'rural_remote',
                    'town_remote',
                    'town_distant'
                )
                then 1
            when community_type is not null
                then 0
        end as is_rural,

        case
            when grades_offered_lo is null then null
            when
                grades_offered_lo in (
                    '01',
                    '02',
                    '03',
                    '04',
                    '05',
                    'PK',
                    'KG'
                )
                then 1
            when coalesce(
                is_grade_pk,
                is_grade_kg,
                is_grade_01,
                is_grade_02,
                is_grade_03,
                is_grade_04,
                is_grade_04,
                is_grade_05
            ) = 1 then 1
            else 0
        end as is_stage_el,

        case
            when grades_offered_lo is null then null
            when
                lower(grades_offered_lo) = 'pk'
                and grades_offered_hi = '06'
                then 0
            when
                lower(grades_offered_lo) = 'kg'
                and grades_offered_hi = '06'
                then 0
            when coalesce(is_grade_06, is_grade_07, is_grade_08) = 1
                then 1
            when (
                grades_offered_lo in ('06', '07', '08')
                or grades_offered_hi in ('06', '07', '08')
            )
                then 1
            else 0
        end as is_stage_mi,

        case
            when grades_offered_lo is null then null
            when coalesce(
                is_grade_09,
                is_grade_10,
                is_grade_11,
                is_grade_12,
                is_grade_13
            ) = 1 then 1
            when grades_offered_hi in (
                '09',
                '10',
                '11',
                '12'
            ) then 1
            else 0
        end as is_stage_hi,

        coalesce(ssby.students_total, 0) as students, -- create aliases for all race groups. Force 0 in case student count is null
        coalesce(ssby.student_am_count, 0) as student_am,
        coalesce(ssby.student_as_count, 0) as student_as,
        coalesce(ssby.student_hi_count, 0) as student_hi,
        coalesce(ssby.student_bl_count, 0) as student_bl,
        coalesce(ssby.student_wh_count, 0) as student_wh,
        coalesce(ssby.student_hp_count, 0) as student_hp,
        coalesce(ssby.student_tr_count, 0) as student_tr,

        student_am
        + student_as
        + student_hi
        + student_bl
        + student_wh
        + student_hp
        + student_tr as sum_of_all_races, -- use as denominator for most calcs

        (sum_of_all_races - student_tr)
        as sum_of_all_races_no_tr -- shortcut to make a non-tr denominator

        student_am +
        student_hi +
        student_bl +
         student_hp AS nhpi, -- the URG group - Native American + Hispanic + Black + Hawaiian


        CASE -- if the sum-of-all-races matches total_students then use total_students as denomninator
            WHEN ssby.students_total = sum_of_all_races
            THEN nhpi / students::float        
        END AS urm_percent, -- this is the "classic" definition


        CASE -- if sum-of-all-races is at least 70% of total students (i.e. seems like enough data we can use it)
                -- then use sum-of-all-races as denominator and call this the "true" urg_percent. 
            WHEN .7 <= sum_of_all_races / students::float    
            THEN nhpi / sum_of_all_races::float              
        END AS urm_percent_true,

        CASE -- if we have non-zero student counts for any non-tr races, then calcluate the urm_perent WITHOUT tr
            WHEN 0 < sum_of_all_races_no_tr
            THEN nhpi / sum_of_all_races_no_tr::FLOAT
        END AS urm_percent_no_tr,

        case 
            when total_frl_eligible is null 
                or total_students is null 
                or total_frl_eligible > total_students 
            then null
            else total_frl_eligible / total_students::float 
        end                                                                                 as frl_eligible_percent,    
        case 
            when total_frl_eligible is null 
                or total_students is null 
            then null 
            when (total_frl_eligible / total_students::float) > 0.5
            then 1 
            else 0 
        end                                                                                 as is_high_needs,

        min(school_year) as first_survey_year,
        max(school_year) as survey_year

    from {{ ref('base_dashboard__school_stats_by_years') }}
    {{ dbt_utils.group_by(37) }}
)

select *
from school_stats_by_years
