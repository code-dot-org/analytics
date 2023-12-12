/*
    Prototype for school_level AP data staging transformation.
    Uses same (un)pivoting technique as agg_ap_exam_results, just with more columns fixed, and a whole bunch more renaming.
    
*/
with
    unpivoted_data as (
        -- see macros/unpivot_dynamic
        {{ unpivot_big_table("stg_2022_school_level_ap_exam_results_raw", 7) }}
    ),
    intermediate as (
        select
            examyr4 as exam_year,
            country_descr as country,
            lpad(ai_code, 6, '0') as ai_code,
            high_school as high_school_name,
            state_abbrev as state,
            analysis_school_type as school_type,
            case
                when subject_nm = 'Computer Sci A'
                then 'csa'
                when subject_nm = 'Computer Sci Prin'
                then 'csp'
                else 'ERROR'
            end exam,
            orig_col_name,

            left(
                orig_col_name,
                length(orig_col_name) - strpos(reverse(orig_col_name), '_')
            ) as demographic_group_raw,
            case
                when demographic_group_raw = 'black_african_american'
                then 'black'
                when demographic_group_raw = 'asian_asian_american'
                then 'asian'
                when demographic_group_raw = 'other_race_ethnicity'
                then 'other'
                when demographic_group_raw = 'hispanic_latino'
                then 'hispanic'
                when demographic_group_raw = 'race_ethnicity_no_response'
                then 'race_no_response'
                when demographic_group_raw = 'american_indian_alaska_native'
                then 'amind'
                when demographic_group_raw = 'two_or_more_races'
                then 'twomore'
                when demographic_group_raw = 'white'
                then 'white'
                when demographic_group_raw = 'native_hawaiian_other_pacific_islander'
                then 'nhpi'
                else demographic_group_raw  -- Fresh/soph/junior/senior/overall/male/female/another_gender
            end demographic_group,
            case
                when demographic_group in ('male', 'female', 'gender_another')
                then 'gender'
                when
                    demographic_group in (
                        'amind',
                        'asian',
                        'black',
                        'hispanic',
                        'nhpi',
                        'twomore',
                        'white'
                    )
                then 'race'
                when demographic_group in ('overall')
                then 'total'
                when demographic_group in ('freshman', 'sophomore', 'senior', 'junior')
                then 'grade_level'
                else 'ERROR'
            end demographic_category,

            right(
                orig_col_name, strpos(reverse(orig_col_name), '_') - 1
            ) as score_category,

            case
                when score_category in (1, 2, 3, 4, 5)
                then score_category
                when score_category in ('total')
                then 'total'
                else 'ERROR'
            end score_cat_process,

            orig_value as num_students
        from unpivoted_data
    )
select *
from intermediate
