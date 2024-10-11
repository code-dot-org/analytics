with heavy_user_schools as (
    select * from {{ ref('ballmer_heavy_user_schools') }}
),

crosswalk as (
    select * from {{ ref('ap_aicode_nces_crosswalk')}}
),

ledgers as (
    select * from {{ ref('ap_ledgers')}}
),

ballmer_csp_heavy_u_audit as (
    select 
        exam,
        exam_year,
        ledger_group,
        ai_code,
        ledgers.name,
        urm_percent,
        title_i,
        high_needs,
        is_rural
    from 
        heavy_user_schools
        left join crosswalk on heavy_user_schools.school_id = crosswalk.nces_id
    where 
        school_year = '2023-24' and
        exam = 'csp' and
        ledger_group = 'cdo_audit'
)

select * from ballmer_csp_heavy_u_audit



