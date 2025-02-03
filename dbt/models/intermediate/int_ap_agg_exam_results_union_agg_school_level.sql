/*

    This model unions together: 
    1. all of the aggregated ap exam results 
    2. the computation of the 'cdo_audit' aggregate group from school-level exam results

    This makes almost the complete "classic" aggregrated exam results from College Board data.
    There is more downstream work to do, to compute the race_no_response group and URGs.

*/
with 
agg_exam_results as (
    select
        'college board' as source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
        demographic_category,
        demographic_group,
        score_category,
        score_of,
        num_students

    from {{ref('stg_external_datasets__ap_agg_exam_results')}}
)
, cdo_audit_group_from_school_level_results as (

    select
        'college board' as source,
        exam_year, 
        null                as pd_year,
        case 
            when exam = 'csp' then 'csp_audit'
            when exam = 'csa' then 'csa_audit'
        end  as reporting_group, 
        null                as rp_id,
        exam, 
        demographic_category, 
        demographic_group, 

        score_category, 
        score_of, 
        sum(num_students)   as num_students

    from {{ ref('int_ap_school_level_results') }}
    where (reporting_group != 'csa_audit' or exam_year != '2022') --we didn't have a CSA offering for 2022, so results are not comparable
    {{ dbt_utils.group_by(10) }}
)
, final as (
    select * from agg_exam_results
    union all
    select * from cdo_audit_group_from_school_level_results
) 
select * 
from final



