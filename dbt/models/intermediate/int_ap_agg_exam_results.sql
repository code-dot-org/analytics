/*

    This model unions together: 
    1. all of the aggregated ap exam results 
    2. computes the 'cdo_audit' aggregate group from code.org school-level exam results

*/
with agg_exam_results as (
    select
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
        --orig_col_name,
        --demographic_group_raw,
        --score_category_raw,
        demographic_category,
        demographic_group,
        score_category,
        score_of,
        num_students

    from {{ref('stg_external_datasets__ap_agg_exam_results')}}

    union all


    -- 2. compose 'cdo_audit' agg reports from school_level_data
    -- the order of these fields MUST match the order listed above.
    select
        exam_year, 
        null                as pd_year,
        'cdo_audit'         as reporting_group, -- in theory this should be run through the macro that normalizes these values
        null                as rp_id,
        exam, 
        demographic_category, 
        demographic_group, 

        score_category, 
        score_of, 
        sum(num_students)   as num_students

    from {{ ref('stg_external_datasets__ap_school_level_exam_results') }}
    {{ dbt_utils.group_by(9) }}
)
, final as (
    select 
        'college board' as source,
        * 
    from agg_exam_results
) 
select * 
from final



