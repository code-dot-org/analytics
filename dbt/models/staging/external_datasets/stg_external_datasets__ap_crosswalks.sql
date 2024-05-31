/*
   
*/
with all_data as (

    {% set years = ['2016_2022', '2023'] %} 

    {% for year in years %}
        select * from {{ ref('base_external_datasets__ap_crosswalk_'~year) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)
, fixed as (
    select
        exam_year,
        source,
        {{ pad_school_id('nces_id') }}  as nces_id,
        {{ pad_ai_code('ai_code') }}    as ai_code,
        school_name,
        city,
        state,
        {{ pad_zipcode('zip') }} as zip
    from all_data
)
select *
from fixed
