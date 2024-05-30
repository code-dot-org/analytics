/*

*/

with agg_exam_results as (
    select * from {{ ref('int_ap_agg_exam_results') }}
)
, bhnapi as (
    select
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
    {{ dbt_utils.group_by(9) }}  
)
, wh_as_other as (
    select
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
    {{ dbt_utils.group_by(9) }}  
)
, final as (
    select 
        'college board' as source,
        * 
    from agg_exam_results

    union all

    select
        'calculated' as source,
        *
    from bhnapi

    union all

    select
        'calculated' as source,
        *
    from wh_as_other
)
select * 
from final

