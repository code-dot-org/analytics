/*
    The purpose of this intermediate table is to: 

    (1) reshape the base_ tables of aggregate exam results into 7 columns (see docs) and 
    (2) union them together
    (3) normalize the values (e.g. APCSA, AP Computer Science A, etc. --> 'csa')

    Note: the unpivot macro assumes that the first 5 columns are fixed. See documentation for base tables that describe what those are and in what order.

    How to Update:
    (0) (Assumption) you have created a new base or staging table for the aggregate (not school-level) data you want to onboard.
    (1) Add the new table to the loop
    (2) build the model
    (3) check all values derived from macros for 'UNEXPECTED' values -- these are values that the normalization macros weren't expecting
    (4) adjust any macros to handle the new values.
    (5) repeat from step 2 until there are no more UNEXPECTED values and all tests pass


*/
with unpivoted_data as (
    -- NOTE: these should be unionable because we're pivoting around the same set of 5 columns. i.e. the width of the base tables shouldn't matter as long as the first 5 columns are the same.

    
    {% set files = ['base_external_datasets__ap_agg_exam_results_2017_2022',
                    'base_external_datasets__ap_agg_exam_results_2023',
                    'base_external_datasets__ap_agg_exam_results_2024',
                    'stg_external_datasets__ap_public_data'] %} 

    {% for file in files %}
        {{ unpivot_big_table(file, 5)}}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}

)

, normalized_values AS (
    SELECT

        exam_year                               as exam_year,
        pd_year                                 as pd_year,
        case
            when exam_group = 'cdo_audit' then 'csp_audit' -- specifying cdo_audit as csp for 2022 and earlier
            else {{ ap_norm_exam_group('exam_group') }}  
            end as reporting_group,                    -- exam_group is the name of the aggregate report group, the value of which is inconsistently given each year.  eg. "csp_audit_and_users" == "csp_ballmer" and so on.  This macro normalizes the values.
        rp_id                                   as rp_id,
        {{ ap_norm_exam_subject('exam') }}      as exam,

        orig_col_name,
        {{ ap_split_column('orig_col_name') }},                   -- creates two columns named: demographic_group_raw and score_category_raw e.g. 'black_1' -> demographic_group_raw = 'black' and score_category_raw=1
        {{ ap_norm_demographic_group('demographic_group_raw') }}, -- creates two cols named demographic_group and demographic_category. e.g 'black' demographic_group='black', demographic_category='race'
        {{ ap_norm_score_category('score_category_raw')}},        -- creates two cols for e.g. '1' score_category='detail', score_of = 1
        orig_value as num_students

    FROM unpivoted_data
)

SELECT
    * 
FROM normalized_values