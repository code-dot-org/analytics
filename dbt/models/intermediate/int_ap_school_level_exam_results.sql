/*
    This intermediate table unions together all the school-level AP exam results.
    It's designed to allow for different years of available data for CSP and CSA.

    In theory, if (1) the staging table for the exam + year exists and (2) it is properly formed with
    the correct number and order of columns and (3) it is named according to the established naming convention, 
    then all the analytics engineer needs to do is add the appropriate year to the list-of-years for the 
    relavant exam (CSP or CSA) in the code below.

    -- see column list
    -- assume naming convention: stg_external_datasets__ap_school_level_exam_results_[exam]_[year]
    -- code simply unions together any/all tables that follow that naming convention for any years
    -- listed in the given arrays.

    -- Goal: Orgininally this jinja was in a macro but I thought that was too much indirection for
    -- the future analyst who will need to update this code.
    -- Process: when a new staging table for AP data is ready to be unioned onto existing data
    -- assuming
    -- (1) that you have all the columns listed below
    -- (2) you have a table that follows the naming conventon
    -- THEN simply add the year to the array of years for the appropriate exam, csp_years or csa_years

    -- The jinja code composes a select statement with the columns in the same order, and unions all the tables together.
    -- highlight the jinja and hit compile to see!

*/

-- This is effectively the template for school-level data
{% set columns = [
    'exam_year::integer', 
    'country', 
    'ai_code', 
    'high_school_name', 
    'state', 
    'school_type', 
    'exam::text', 
    'demographic_group::text', 
    'demographic_category::text', 
    'score_category::text', 
    'score_of::text', 
    'num_schools::integer', 
    'num_students::integer'
] %}

-- Union together all years for which we have school-level data
-- Add any new years to the array ap_years
{% set ap_years = ['2022', '2023'] %}
{% for year in ap_years %}
    select
        {% for column in columns %}
        {{ column }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ ref('stg_external_datasets__ap_school_level_exam_results_'~ year) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
