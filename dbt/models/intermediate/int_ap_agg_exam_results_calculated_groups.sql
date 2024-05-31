/*
    This model computes some different race/ethnic groups necessary for both reporting and for computing the URG numbers for the final summary report.
    Hierarchically it is downstream from `int_ap_agg_exam_results` and builds on those data.
    

    NOTE: this model could theoretically be part of int_agg_exam_results itself, but is a separate model right now because building int_agg_exam_results
    with these additional CTEs performed horribly.  It built 20x faster as two separate models unioned together.
*/

with agg_exam_results as (
    select * from {{ ref('int_ap_agg_exam_results') }}
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
, final as (
    select *
    from agg_exam_results

    union all

    select *
    from bhnapi

    union all

    select *
    from wh_as_other
)
select * 
from final

