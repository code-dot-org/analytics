/*
    AP school level exam results staging. This does the following:

    1. unpivot the wide raw school-level exam results table into a long table - fixing the first 8 columns, and all subsequent columns pivoted into key-value pairs: orig_col_name | orig_value
    2. union any/all of these ^^^ together
    3. rename columns and normalize the values

    Testing and Clean up: 
    a. Look for values with 'UNEXPECTED' - these are new values or encodings in the college board data - we should add them to the macros that normalize these values
       
    b. MAKE SURE you identify the the "less-than-10-aggregate" school record to see how many schools are included in that aggregate 
       AND check that in your resulting data there are records where num_schools equals that number.
       
       There is a macro that produces the num_schools field, which looks for a school_name like '%LESS THAN 10%N=%' 
       and extracts the n value from the string, every other school is num_schools=1.  If the school name DOES NOT conform to that pattern (CB has a history of changing things)
       the preferred method would be update the macro to handle the new pattern.
*/
with unpivoted_data as (

    {% set years = ['2022', '2023'] %} 

    {% for year in years %}
        {{ unpivot_big_table('base_external_datasets__ap_school_level_exam_results_'~year, 8)}}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)
,normalized_values as (
    select
        examyr4                 as exam_year,
        country_descr           as country,
        lpad(ai_code, 6, '0')   as ai_code,
        high_school             as high_school_name,
        state_abbrev            as state,
        ap_school_type,
        analysis_school_type,

        -- Macro noramlizes exam name
        {{ ap_norm_exam_subject('subject_nm') }} as exam,

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
        ai_code,
        high_school_name,
        state,
        ap_school_type,
        analysis_school_type,
        exam,
        -- comment out orginal and raw columns from final, but useful for testing
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
)
select * 
from final
