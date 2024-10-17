with 

ledgers_2017_2023 as (
    select * 
    from {{ ref('stg_external_datasets__ap_ledgers_2017_2023') }}
),

ledgers_2024 as (
    select * 
    from {{ ref('stg_external_datasets__ap_ledgers_2024') }}
),

crosswalk as (
    select * 
    from {{ ref('stg_external_datasets__ap_crosswalks') }}
),

all_ledgers as (  
    select * from ledgers_2017_2023
    union all 
    select * from ledgers_2024
),

combined as (
    select distinct 
        all_ledgers.* 
        , crosswalk.school_id
    from all_ledgers 
    left join crosswalk 
        on all_ledgers.ai_code = crosswalk.ai_code
)

select * 
from combined

