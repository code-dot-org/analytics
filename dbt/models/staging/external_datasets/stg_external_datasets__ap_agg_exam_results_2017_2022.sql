/*
    The purpose of this staging table is to: 

    (1) losslessly reshape (unpivot) the super-wide data given by the college board into a longer form
    (2) normalize some of the values and and column headings esp. for race/gender
    (3) present a table for downstream models that shows all of the original values
        along with the processed/normalized ones so the analyst can see more.

    Unpivot should result in a table that looks like this.

    -- Should result in a reshaped table of the raw ap exam results that looks like:
    -- exam_year | pd_year | exam_group  | exam | rp_id  | orig_col_name | orig_value |
    -- ----------|---------|-------------|------|--------|---------------|------------|
    -- 2021      | 2020    | rp_all_time | csa  | 13     | hi_5          | 120        |
    -- etc.

    Further, we use macros on some of these raw columns ^^^ to: (1) split into other dimensions (2) values normalize 
*/
with unpivoted_data as (
    -- see macros/unpivot_dynamic
    {{ unpivot_big_table('base_external_datasets__ap_agg_exam_results_2017_2022', 5)}}
), 


normalized_values AS (
    SELECT

        exam_year,
        pd_year,
        {{ ap_norm_exam_group('exam_group') }} as exam_group, --TO DO: macro to norm this
        rp_id,
        --exam,
        {{ ap_norm_exam_subject('exam') }} as exam,
        orig_col_name,
        {{ ap_split_column('orig_col_name') }},                   -- splits e.g. 'black_1' into two cols demographic_group_raw = 'black' and score_category_raw=1
        {{ ap_norm_demographic_group('demographic_group_raw') }}, -- produces two cols for e.g 'black' demographic_group='black', demographic_category='race'
        {{ ap_norm_score_category('score_category_raw')}},        -- produces two cols for e.g. '1' score_category='detail', score_of = 1
        orig_value as num_students

    FROM unpivoted_data
)
SELECT
    exam_year::text,
    pd_year::text,
    exam_group::text,
    rp_id::text,
    exam::text,
    orig_col_name,
    demographic_group_raw,
    score_category_raw,
    demographic_category::text,
    demographic_group::text,
    score_category::text,
    score_of::text,
    num_students::integer

FROM normalized_values
