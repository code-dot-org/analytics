/* 
    It's assumed that this table: analysis.ap_crosswalk_2024 contains the load
    of any/all data files containing ledger data for 2024 (=2023-24 school year)
    see: ap_ledger_ingest code.

    ANNUAL TASK:
    DUPLICATE THIS FILE each year, replace the source, ensure correct columns are being reported.
*/ 

with all_crosswalks AS (
    select * 
    from {{source('external_datasets','ap_crosswalk_2024')}}
)
select
    *
from all_crosswalks