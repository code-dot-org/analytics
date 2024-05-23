/*
    AP school level exam results staging

    1. unpivot the wide table into a long table
        1a. note: the unpivot macro APPEARS to replace null integers with 0s...but I don't see where/why
    
    2. MAKE SURE you identify the the "less-than-10-aggregate" school record to see how many schools are included in that aggregate 
       AND check that in your resulting data there are records where num_schools equals that number.
       
       There is a macro that produces the num_schools field, which looks for a school_name like '%LESS THAN 10%N=%' 
       and extracts the n value from the string, every other school is 1.  If the school name DOES NOT conform to that pattern (CB has a history of changing things)
       the preferred method would be update the macro to handle the new pattern.

    3. rename columns, including using macro to split e.g. "black_1" into demographic_group and score_category colums
        There is a macro that does this splitting on the original raw column name into two new columns AND normalizes the values

    4. normalize values - use macros as much as possible.  
        If a new value appears that that the macro can't handle update the macro will fail loudly with an 'ERRROR - [some message]'
    
    5. Clean up: 
    5.a. Look for values with 'ERROR' - these are new values or encodings in the college board data - we should add them to the macros that normalize these values
    5.b. Ensure all column necessary for later unioning are present


*/
with
    unpivoted_data as (
        -- see macros/unpivot_dynamic.sql
        {{ unpivot_big_table("base_external_datasets__ap_school_level_exam_results_csa_2023_DELETEME", 7) }}
    )
    ,renamed_cols as (
        select
            examyr4 as exam_year,
            country_descr as country,
            lpad(ai_code, 6, '0') as ai_code,  --note: this will make blank '' ai_code = '000000'
            high_school as high_school_name,
            state_abbrev as state,
            analysis_school_type as school_type,

            'csa'::varchar as exam_subject, -- this dataset is entirely csa
            orig_col_name, --keep for sanity checking, remove from final output

            -- Macro call to split `orig_col_name` into two fields: demographic_group_raw, and score_group_raw
            {{ ap_split_column('orig_col_name') }},
            
            orig_value as num_students
        from unpivoted_data
    ) 
    ,normalized_values as (
        select
            exam_year,
            country,
            ai_code,
            high_school_name,
            state,
            school_type,

            -- Macro noramlizes exam name
            {{ ap_norm_exam_subject('exam_subject') }} as exam,

            orig_col_name,
            demographic_group_raw,
            score_category_raw,

            -- Macro that splits demographic_group_raw into two new columns named: demographic_group, demographic_category
            --      AND it also normalizes the vales values (e.g. "black_african_american" --> 'black')
            {{ ap_norm_demographic_group('demographic_group_raw') }}, 
           
            -- Macro that splits score_category_raw into two new columns named: score_category (detail|total), and score_of (1-5, NULL)
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
