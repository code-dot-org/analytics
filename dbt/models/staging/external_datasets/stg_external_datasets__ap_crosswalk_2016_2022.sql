/*
   
*/
with all_data as (
    select * from {{ ref('base_external_datasets__ap_crosswalk_2016_2022') }}
)
,fixed as (select
    exam_year,
    source,
    {{ pad_school_id('nces_id') }}  as nces_id,
    {{ pad_ai_code('ai_code') }} as ai_code,
    name as school_name,
    city,
    state,
    {{ pad_zipcode('zip') }} as zip
from all_data
)
select
    *
from fixed
