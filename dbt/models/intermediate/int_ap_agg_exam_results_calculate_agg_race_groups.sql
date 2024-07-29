/*
    This model computes some different race/ethnic groups (bhnapi and wh_as_other) that are necessary 
    for both reporting in general, and specifically for computing the URG numbers for the final summary report.

    

    NOTE: this model computes ONLY these separate groups (as opposed to a union of these groups onto the existing agg reports)
*/

with agg_exam_results as (
    select * from {{ ref('int_ap_agg_exam_results_calculate_race_no_response') }}
)
, bhnapi as (
    select
        'calculated' as source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
        'calc_urg' as demographic_category,
        'bhnapi' as demographic_group,
        score_category,
        score_of,
        sum(num_students) as num_students
    from agg_exam_results a
    where a.demographic_group IN ('black','hispanic','american_indian','hawaiian')
    {{ dbt_utils.group_by(10) }}  
)
, wh_as_other as (
    select
        'calculated' as source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
        'calc_urg' as demographic_category,
        'wh_as_other' as demographic_group,
        score_category,
        score_of,
        sum(num_students) as num_students
    from agg_exam_results a
    where a.demographic_group IN ('white','asian','other_race')
    {{ dbt_utils.group_by(10) }}  
)
, calc_urg_no_response as ( 
    select 
        source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
        'calc_urg' as demographic_category,
        demographic_group,
        score_category,
        score_of,
        num_students    
    from  agg_exam_results
    where demographic_category = 'race'
    and demographic_group = 'race_no_response'
)
, agg_exam_results_with_new_groups as (
    select * from bhnapi
    union all
    select * from wh_as_other
    union all
    select * from calc_urg_no_response
)
select * 
from agg_exam_results_with_new_groups

