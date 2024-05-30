/* 
    Pull in historical AP ledgers.
    Done from a one-time pull from analyis.ap_ledger_raw.
    See: ap_ledger_ingest.sql
*/ 

with all_ledgers AS (
    select * 
    from {{source('dashboard_analysis','stg_ap_ledgers_raw_2017_2021')}}
)
select
    exam_year,
    school_year,
    exam,
    ledger_group,
    ai_code,
    school_name,
    city,
    state,
    country,
    provider_syllabus
from all_ledgers