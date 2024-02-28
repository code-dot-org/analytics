{% macro ap_norm_exam_subject(column_name) %}
    case
        when {{ column_name }} = 'Computer Sci A' then 'csa'
        when {{ column_name }} = 'Computer Sci Prin' then 'csp'
        else 'ERROR unhandled value: ' || {{column_name }}
    end
{% endmacro %}


/*
    Assumption: the raw column names after cleaning will contain a group+score_category e.g. 'black_1' (black score of 1).
    Sometimes it's a long-form value like e.g. 'black_african_american_1' etc.

    This macro creates two new fields wherever it is inserted.
    It is intended to be used AP exam score staging models
*/
{% macro ap_split_column(orig_col_name)%}
        left(
            {{ orig_col_name }}, 
            length({{ orig_col_name }}) - strpos(reverse({{ orig_col_name }}), '_')
        ) as demographic_group_raw,
        right(
            orig_col_name, strpos(reverse(orig_col_name), '_') - 1
        ) as score_category
{% endmacro%}




/*
    process the raw demographic group and produce two new columns:
    1. demographic_group (black, white, hispanic...male, female...overall...freshman etc.)
    2. demographic_category (race | gender | total | grade_level)
*/
{% macro ap_norm_demographic_group(demographic_group_raw) %}
case
    when {{ demographic_group_raw }} = 'black_african_american' then 'black'
    when {{demographic_group_raw}} = 'asian_asian_american' then 'asian'
    when {{demographic_group_raw}} = 'other_race_ethnicity' then 'other'
    when {{demographic_group_raw}} = 'hispanic_latino' then 'hispanic'
    when {{demographic_group_raw}} = 'race_ethnicity_no_response' then 'race_no_response'
    when {{demographic_group_raw}} = 'american_indian_alaska_native' then 'amind'
    when {{demographic_group_raw}} = 'two_or_more_races' then 'twomore'
    when {{demographic_group_raw}} = 'white' then 'white'
    when {{demographic_group_raw}} = 'native_hawaiian_other_pacific_islander' then 'nhpi'
    else {{demographic_group_raw}} -- default, if it's already one of the expected values below we don't need to explicitly check for it'black'
end as demographic_group
-- Finally, categorize into demographic_category
, case
    when demographic_group in ('male', 'female', 'gender_another') then 'gender'
    when demographic_group in ('amind', 'asian', 'black', 'hispanic', 'nhpi', 'twomore', 'white') then 'race'
    when demographic_group in ('overall') then 'total'
    when demographic_group in ('freshman', 'sophomore', 'senior', 'junior') then 'grade_level'
    else 'ERROR unkown group cateogry for: '|| demographic_group
end as demographic_category
{% endmacro %}
