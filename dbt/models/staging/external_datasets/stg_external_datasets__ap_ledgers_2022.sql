/*
   
*/
with all_data as (
    select * from {{ ref('base_external_datasets__ap_ledgers_2022') }}
)
select 
    exam_year,
    school_year,
    exam,
    ledger_group,
    {{ pad_ai_code('ai_code') }} as ai_code,
    school_name,
    city,
    state,
    country,
    provider_syllabus
from all_data
    
