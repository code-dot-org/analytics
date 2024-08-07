/* 
    It's assumed that this table: analysis.stg_ap_ledgers_raw_2023 contains the load
    of any/all data files containing ledger data for 2023 (=2022-23 school year)
    see: ap_ledger_ingest code.
*/ 

with all_ledgers AS (
    select * 
    from {{source('external_datasets','stg_ap_crosswalk_2016_2022')}}
)
select
    exam_year,
    source,
    nces_id,
    ai_code,
    name as school_name,
    city,
    state,
    zip 
from all_ledgers