/*
    To onboard new ledgers.
    1. add source and base table (see instructions in: base_external_datasets__ap_ledgers_20XX.sql )
    2. add the new year to the array of years[] below.  
    3. Build and debug.
   
*/
with all_data as (

    {% set years = ['2017_2021', '2022', '2023'] %} 

    {% for year in years %}
        select * from {{ ref('base_external_datasets__ap_ledgers_'~year) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
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



