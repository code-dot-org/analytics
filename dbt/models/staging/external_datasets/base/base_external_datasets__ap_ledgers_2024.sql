/* 
    Loading this as-is and then doing the cleaning work in a staging table. 

    Issues
    - non standard exam names
    - non standard zip/state/district_id
    - 0 rather than null for district_id
    - Not affiliated rather than null for district
    - State sometimes contains country info

    It's assumed that this table: analysis.stg_ap_ledgers_raw_2024 contains the load
    of any/all data files containing ledger data for 2024 (=2023-24 school year)
    see: ap_ledger_ingest code.
*/ 

with all_ledgers AS (
    select * from {{source('external_datasets','stg_ap_ledgers_raw_2024')}}
)

select *
from all_ledgers

