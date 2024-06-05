/* 
    It's assumed that this table: analysis.stg_ap_ledgers_raw_2023 contains the load
    of any/all data files containing ledger data for 2023 (=2022-23 school year)
    see: ap_ledger_ingest code.

    The raw data frequently has a duplicate or two slipped in there.  some lightweight de-duping is done here at the base.

    DUPLICATE THIS FILE each year, replace the source, ensure correct columns are being reported.

*/ 

with all_ledgers AS (
    select
        exam_year,
        school_year,
        exam,
        ledger_group,
        ai_code,
        max(school_name) as school_name,
        max(city) as city,
        max(state) as state,
        max(country) as country,
        max(provider_syllabus) as provider_syllabus
    from {{source('external_datasets','stg_ap_ledgers_raw_2023')}}
    {{dbt_utils.group_by(5)}}
)
select *
from all_ledgers