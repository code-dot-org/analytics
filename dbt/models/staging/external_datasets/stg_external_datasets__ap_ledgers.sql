/*
    This table unions together all YoY ledger data from the base tables.

    ANNUAL TASK
    
    To onboard new ledgers.
    (0) (Assumption) you have created a new base table called base_external_datasets__ap_ledgers_'~year for the year in question.
    (1) add the new year to the array of years[] below.  
    (2) Build and debug.
    (3) repeat until you get a clean build passing all tests

*/

with all_data as (

    {% set years_base = ['2017_2021', '2022', '2023'] %} 

    {% for year in years_base %}
        select * from {{ ref('base_external_datasets__ap_ledgers_'~year) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}

    union all 
    select * from {{ ref('stg_external_datasets__ap_ledgers_2024') }}
)

select 
    exam_year,
    school_year,
    exam,
    ledger_group,
    {{ pad_ai_code('ai_code') }}    as ai_code,
    school_name,
    city,
    state,
    country,
    provider_syllabus
from all_data