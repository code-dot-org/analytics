/*
   Union together all of the ai/nces crosswalk base tables.

   ANNUAL TASK:
   0. (Assumption) you have created a new base table named base_external_datasets__ap_crosswalk_'~year  for the year in question.
   1. add a new year to the years[] array 
   2. Build the model
   3. Investigate data, ensure it passes tests 
   4. repeat until done.
*/
with 

all_data as (

    {% set years = ['2016_2022', '2023', '2024'] %} 

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
        {{pad_school_id('nces_id')}}                                    as school_id,                                                       
        {{ pad_ai_code('ai_code') }}                                            as ai_code,
        school_name                                                     as school_name,
        city                                                             as city,
        state                                                            as state,
        {{ pad_zipcode('zip') }} as zip,
        row_number() over (
            partition by {{ pad_ai_code('ai_code') }} 
            order by exam_year desc
        )                                                                       as row_num
    from all_data
)

select *
from fixed
where row_num = 1 -- this pulls the most recent match