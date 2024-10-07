/* 
    It's assumed that this table: analysis.stg_ap_crosswalk_2024 contains the load
    of any/all data files containing crosswalk data for 2024 (=2023-24 school year)
    see: ap_ledger_ingest code.

    ANNUAL TASK:
    DUPLICATE THIS FILE each year, replace the source, ensure correct columns are being reported.
*/ 

with all_crosswalks AS (
    select * 
    from {{source('external_datasets','stg_ap_crosswalk_2024')}}
)
select
    exam_year,
    source,
    nces_id,
    ai_code,
    school_name,
    city,
    state,
    zip 
from all_crosswalks