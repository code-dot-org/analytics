/*
    Prototype for school_level AP data staging transformation.
    Uses same (un)pivoting technique as agg_ap_exam_results, just with more columns fixed, and a whole bunch more renaming.

*/
with unpivoted_data as (
    -- see macros/unpivot_dynamic
    {{ unpivot_big_table('base_external_datasets__ap_school_level_exam_results_csp_2022_DELETEME', 7) }}
) 
, renamed_cols as (
    select
        examyr4 as exam_year,
        country_descr as country,
        high_school as high_school_name,
        state_abbrev as state,
        analysis_school_type as school_type,
        subject_nm as exam,
        orig_col_name,
        lpad(ai_code, 6, '0') as ai_code, --keep for sanity checking
        -- Macro call to split `orig_col_name` into two fields: demographic_group_raw, and score_category_raw
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

        -- Macro normalizes exam name to 'csp' or 'csa', does not insert a field name so you need to add alias
        {{ ap_norm_exam_subject('exam') }} as exam,
        orig_col_name,
        demographic_group_raw,
        score_category_raw,

        -- Macro that splits demographic_group_raw into two new columns: demographic_group, demographic_category
        --      AND inserts those aliases into the code
        --      AND it also normalizes the vales values (e.g. "black_african_american" --> 'black')
        {{ ap_norm_demographic_group('demographic_group_raw') }}, 
        
        -- Macro that splits score_category_raw into two new columns: score_category (detail|total), and score_of (1-5, NULL)
        --  AND inserts those twoo aliases
        {{ ap_norm_score_category('score_category_raw')}},

        -- Macro finds school called e.g. '%LESS THAN 10%N=123' and extracts 123 as num_schools, otherwise num_schools=1
        {{ ap_extract_n_schools_from_aggregate('high_school_name') }} as num_schools,

        num_students
    from renamed_cols
)
select
    exam_year,
    country,
    ai_code,
    high_school_name,
    state,
    school_type,
    exam,
    -- orig_col_name,
    -- demographic_group_raw,
    -- score_category_raw,
    demographic_group,
    demographic_category,
    score_category,
    score_of,
    num_schools,
    num_students
from normalized_values
