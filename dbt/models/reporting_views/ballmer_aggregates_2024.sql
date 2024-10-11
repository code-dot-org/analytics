{#
model: 
auth: cory
notes: This creates the aggregations required for College Board for 2024. Future years will likely be similar but might require annual updates.

#}

with heavy_user_schools as (
    select * from {{ ref('ballmer_heavy_user_schools') }}
    where school_year = '2023-24'
)

, ledgers as (
    select * from {{ ref('dim_ap_ledgers')}}
    where school_year = '2023-24'
)

, schools as (
    select * from {{ref('dim_schools')}}
)

, combined as (
    select 
        exam,
        exam_year,
        ledger_group,
        ai_code,
        school_id
        schools.school_name,
        ledgers.school_name as ledger_school_name,
        schools.state,
        is_title_i,
        frl_eligible_percent,
        is_rural,
        urg_percent,
        is_high_needs,
        case when is_title_i = 1
            or frl_eligible_percent > 0.4
            or is_rural = 1
            or urg_percent > 0.3
            then 1
            else 0
        end afe_eligible
        heavy_user_school_flag
    from 
        ledgers
        left join heavy_user_schools on heavy_user_schools.school_id = ledgers.school_id
        left join schools on ledgers.school_id = schools.school_id
)

, csp_audit as (
    select * from combined
    where exam = 'csp' and ledger_group = 'cdo_audit'
)

select * from csp_audit

, csa_audit as (
    select * from combined
    where exam = 'csa' and ledger_group = 'cdo_audit'
)

, csp_heavy as (
    select * from combined
    where exam = 'csp' and heavy_user_school_flag = 1
)

, csa_heavy as (
    select * from combined
    where exam = 'csa' and heavy_user_school_flag = 1
)

, csp_ballmer as (
    csp_audit 
    UNION all
    csp_heavy
)

, csa_ballmer as (
    csa_audit
    UNION all
    csa_heavy
)

, afe_heavy_csp as (
    select * from csp_heavy
    where afe_eligible = 1
)

, afe_heavy_csa as (
    select * from csa_heavy
    where afe_eligible = 1
)

, final as (
    select ai_code,
    school_name,
    state,
    case when ai_code in csp_ballmer.ai_code then 1 else 0 end as csp_ballmer,
    case when ai_code in csa_ballmer.ai_code then 1 else 0 end as csa_ballmer,
    case when ai_code in csp_heavy.ai_code then 1 else 0 end as csp_heavy_user,
    case when ai_code in csa_heavy.ai_code then 1 else 0 end as csa_heavy_user,
    case when ai_code in afe_heavy_csp.ai_code then 1 else 0 end as csp_heavy_afe,
    case when ai_code in csa_heavy.ai_code then 1 else 0 end as csa_heavy_afe
)

select * from final
