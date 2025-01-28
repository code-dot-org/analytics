/*
    AP school level exam results staging. This does the following:

    (1) reshape the base_ tables of aggregate exam results into 10 columns (see docs) and 
    (2) union them together
    (3) normalize the values (e.g. APCSA, AP Computer Science A, etc. --> 'csa')

    Note: the unpivot macro assumes that the first 8 columns are fixed.  See documentation for base tables that describe what those are and in what order.

    ANNUAL TASK:
    (0) (Assumption) you have created a new base table called bbase_external_datasets__ap_school_level_exam_results_'~year for the year in question.
    (1) add a new exam year to the years [] array.
    (2) build the model
    (3) check all values derived from macros for 'UNEXPECTED' values -- these are values that the normalization macros weren't expecting
    (4) adjust any macros to handle the new values.
    (5) repeat from step 2 until there are no more UNEXPECTED values and all tests pass


*/

with unpivoted_data as (

    {% set years = ['2024'] %} 

    {% for year in years %}
        {{ unpivot_big_table('base_external_datasets__ap_school_level_exam_results_'~year, 7)}}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)

, normalized_values as (
    select
        exam_year                 as exam_year,
        {{country_normalization('country')}}           as country,
        lpad(ai_code, 6, '0')   as ai_code,
        high_school             as high_school_name,
        state_abbr            as state,
        --lower(ap_school_type) as ap_school_type,
        lower(analysis_school_type) as analysis_school_type,

        -- Macro noramlizes exam name
        {{ ap_norm_exam_subject('exam') }} as exam,

        orig_col_name, --keep for sanity checking, remove from final output

        -- Macro call to split `orig_col_name` into two fields: demographic_group_raw, and score_group_raw
        {{ ap_split_column('orig_col_name') }},
        --demographic_group_raw,
        --score_category_raw,

        -- Macro that splits demographic_group_raw into two new columns named: demographic_group, demographic_category
        --      AND it also normalizes the vales values (e.g. "black_african_american" --> 'black')
        {{ ap_norm_demographic_group('demographic_group_raw') }}, 
        
        -- Macro that splits score_category_raw into two new columns named: score_category (detail|total), and score_of (1-5, NULL)
        {{ ap_norm_score_category('score_category_raw')}},

        -- Macro finds school called e.g. '%LESS THAN 10%N=123' and extracts 123 as num_schools, otherwise num_schools=1
        {{ ap_extract_n_schools_from_aggregate('high_school_name') }} as num_schools,
        
        orig_value              as num_students
    from unpivoted_data
)

, final as (
    select
        exam_year,
        country,
        case --removing ai_code for the multi-school aggregate
            when num_schools > 1 then NULL
            else ai_code
        end as ai_code,
        high_school_name,
        state,
        exam,
        -- comment out orginal and raw columns from final, but useful for testing
        --orig_col_name,
        --demographic_group_raw,
        --score_category_raw,
        demographic_group,
        demographic_category,
        score_category,
        score_of,
        num_schools,
        num_students
    from normalized_values
)
select * 
from final
