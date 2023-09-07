/*
    The purpose of this staging table is to: 
    (1) reshape (unpivot) the super-wide data given by the college board into a longer form
    (2) normalize some of the values and and column headings esp. for race/gender
    (3) present a table for processing further in models that shows all of the original values
        along with the processed/normalized ones so the analyst can see more
*/
with
    unpivoted_data as (
        -- see macros/unpivot_dynamic
        {{ unpivot_big_table("seed_2022_agg_ap_exam_results", 2) }}

    -- Should result in a reshaped table of the raw ap exam results that looks like:
    -- tag              | exam                  | orig_col_name | orig_value
    -- -----------------|-----------------------|---------------|------------
    -- csa pd all time  | Computer Science A    | total_1       | 545
    -- heavy_union_audit| Computer Science A    | male_1        | 4586
    -- etc.
    )
select

    '2022' as exam_year,
    "tag",
    case
        when exam = 'Computer Science A'
        then 'csa'
        when exam = 'Computer Science Principles'
        then 'csp'
        when exam = 'Computer Science A or Principles'
        then 'csa_or_csp'
        else 'ERROR'
    end exam,
    orig_col_name,
    -- a little crazy: making use of REVERSE to find the location of the last
    -- underscore in the string
    left(
        orig_col_name, length(orig_col_name) - strpos(reverse(orig_col_name), '_')
    ) as demographic_group,
    case
        when demographic_group in ('male', 'female', 'gender_another') then 'gender'
        when demographic_group in ('amind', 'asian', 'black', 'hispanic', 'nhpi', 'twomore', 'white') then 'race'
        when demographic_group in ('total') then 'total'
        else 'ERROR'
    end demographic_category,
    right(orig_col_name, strpos(reverse(orig_col_name), '_') - 1) as score_category,

    orig_value as num_students

from unpivoted_data
