with all_ledgers as (
    select 
        l.*,
        cw.nces_id
    from {{ ref('stg_external_datasets__ap_ledgers') }} l
    left join {{ ref('ap_aicode_nces_crosswalk') }} cw on cw.ai_code = l.ai_code
)
select
    exam_year,
    school_year,
    exam,
    ledger_group,
    ai_code,
    nces_id,
    school_name,
    city,
    state,
    country,
    provider_syllabus

from all_ledgers
