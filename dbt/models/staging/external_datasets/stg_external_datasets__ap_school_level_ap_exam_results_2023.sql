/*
    AP exam results staging

    1. unpivot the wide table into a long table
        1a. note: the unpivot macro APPEARS to do this --> replace null integers with 0s...but I don't see where/why
    3. ensure you have all necessary columns for a clean unioning later (make this list in a macro?)
    4. normalize values - use macros as much as possible.  If a new value appears that that the macro can't handle update the macro
    

*/
with
    -- source as (
    --     select * 
    --     from {{ ref('base_external_datasets__ap_school_level_exam_results_csp_2023') }}
    -- )
    -- select * FROM source
    unpivoted_data as (
        -- see macros/unpivot_dynamic.sql
        {{ unpivot_big_table("base_external_datasets__ap_school_level_exam_results_csp_2023", 7) }}
    )
    --SELECT * FROM unpivoted_data where orig_col_name LIKE '%black%'
    ,
    renamed_cols as (
        select
            examyr4 as exam_year,
            country_descr as country,
            lpad(ai_code, 6, '0') as ai_code,
            high_school as high_school_name,
            state_abbrev as state,
            analysis_school_type as school_type,
            'csp' as exam, --there is a macro to normalize exam if there is a field with multiple exam names in it
            orig_col_name, --keep for sanity checking

            -- Macro call to split `orig_col_name` into: demographic_group_raw and score_category fields
            {{ ap_split_column('orig_col_name') }},
            orig_value as num_students
        from unpivoted_data
    )
    , normalized_values as (
        select
            exam_year,
            country,
            ai_code,
            high_school_name,
            state,
            school_type,
            exam,
            orig_col_name,
            demographic_group_raw,
            score_category,
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
                else demographic_group_raw  
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
            case
                when score_category in (1, 2, 3, 4, 5)
                then score_category
                when score_category in ('total')
                then 'total'
                else 'ERROR'
            end score_cat_process
        from renamed_cols

    )
select *
from normalized_values
