/*
    The purpose of this staging table is to: 

    (1) losslessly reshape (unpivot) the super-wide data given by the college board into a longer form
    (2) normalize some of the values and and column headings esp. for race/gender
    (3) present a table for downstream models that shows all of the original values
        along with the processed/normalized ones so the analyst can see more.
*/
WITH unpivoted_data AS (
    -- see macros/unpivot_dynamic

    {{ unpivot_big_table('stg_2017_2022_agg_ap_exam_results_raw', 5)}}

    -- Should result in a reshaped table of the raw ap exam results that looks like:
    -- exam_year | pd_year | exam_group  | exam | rp_id  | orig_col_name | orig_value |
    -- ----------|---------|-------------|------|--------|---------------|------------|
    -- 2021      | 2020    | rp_all_time | csa  | 13     | hi_5          | 120        |
    -- etc.
), 
ap_data AS (
SELECT

    exam_year,
    pd_year,
    exam_group,
    rp_id,
    exam,

    orig_col_name,
    -- a little crazy: making use of REVERSE to find the location of the last underscore in the string
    LEFT(orig_col_name, LENGTH(orig_col_name) - STRPOS(REVERSE(orig_col_name), '_')) AS demographic_group_raw,

    CASE --normalize these abbreviations into short-form race names
        WHEN demographic_group_raw = 'am' THEN 'amind'
        WHEN demographic_group_raw = 'as' THEN 'asian'
        WHEN demographic_group_raw = 'bl' THEN 'black'
        WHEN demographic_group_raw = 'hi' THEN 'hispanic'
        WHEN demographic_group_raw = 'hp' THEN 'nhpi'
        WHEN demographic_group_raw = 'tr' THEN 'twomore'
        WHEN demographic_group_raw = 'wh' THEN 'white'
        ELSE demographic_group_raw
    END demographic_group,

    CASE
        WHEN demographic_group IN ('male','female','other_gender') THEN 'gender'
        WHEN demographic_group IN ('amind','asian','black','hispanic','nhpi','twomore','white') THEN 'race'
        WHEN demographic_group IN ('total') THEN 'total'
        ELSE 'ERROR'
    END demographic_category,

    RIGHT(orig_col_name, STRPOS(REVERSE(orig_col_name), '_') - 1) AS score_category,

    orig_value AS num_students

FROM unpivoted_data
)
SELECT
   *
FROM ap_data





