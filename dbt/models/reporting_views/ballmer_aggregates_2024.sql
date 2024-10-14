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

, pd_all_time as (
    select * from {{ref('pd_all_time') }}
)

, combined as (
    select 
        exam,
        exam_year,
        ledger_group,
        ai_code,
        ledgers.school_id,
        schools.school_name,
        schools.state,
        schools.city,
        ledgers.school_name as ledger_school_name, -- for validation
        ledgers.state as ledger_state,
        ledgers.city as ledger_city,
        school_category,
        is_title_i,
        frl_eligible_percent,
        is_rural,
        urg_percent,
        is_high_needs,
        case 
            when is_title_i = 1
            or frl_eligible_percent > 0.4
            or is_rural = 1
            or urg_percent > 0.3
            then 1
            else 0
        end afe_eligible,
        case 
            when heavy_user_school_flag IS NULL 
            then 0 
            else heavy_user_school_flag
        end heavy_user_school_flag
    from 
        ledgers
        left join heavy_user_schools
            on heavy_user_schools.school_id = ledgers.school_id 
            and heavy_user_schools.course_name = ledgers.exam
        left join schools on ledgers.school_id = schools.school_id
)

, csp_audit as ( --Checking if we should remove non-US schools from this list
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csp_audit' as label
    from combined
    where exam = 'csp' and ledger_group = 'cdo_audit'
)

, csa_audit as ( --Checking if we should remove non-US schools from this list
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csa_audit' as label
    from combined
    where exam = 'csa' and ledger_group = 'cdo_audit'
)

, csp_heavy as (
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csp_heavy' as label
    from combined
    where exam = 'csp' and heavy_user_school_flag = 1
)

, csa_heavy as (
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csa_heavy' as label
    from combined
    where exam = 'csa' and heavy_user_school_flag = 1
)

, csp_ballmer as (
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csp_ballmer' as label 
    from 
    csp_audit 
    UNION all
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csp_ballmer' as label  
    from 
    csp_heavy
)

, csa_ballmer as (
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csa_ballmer' as label 
    from 
    csa_audit 
    UNION all
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csa_ballmer' as label  
    from 
    csa_heavy
)

, csp_afe_heavy as (
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csp_afe_eligible' as label
    from csp_heavy
    where afe_eligible = 1
)

, csa_afe_heavy as (
    select ai_code, school_name, state, exam, ledger_group, afe_eligible, heavy_user_school_flag,
    'csa_afe_eligible' as label
    from csa_heavy
    where afe_eligible = 1
)

, query_join as (
  SELECT * FROM csa_ballmer
  union all
  SELECT * FROM csp_ballmer
  union all
  SELECT * FROM csa_heavy
  union all
  SELECT * FROM csp_heavy
  union all
  SELECT * FROM csp_afe_heavy
  union all
  SELECT * FROM csa_afe_heavy
)

, final as (
    select
    distinct 
    label,
    ai_code,
    school_name,
    state
    from query_join

    union all
    select * from {{ref('pd_all_time')}}

)

select * from final
