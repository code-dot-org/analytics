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
---------------------------------------------------------- 
-- several CTEs to compute race_no_response
-- 1. compute sum of all races for each score 1-5 
-- 2. find total num students for each score 1-5 
-- 3. find existing dataset with a `race_no_response` field already
-- 4. compute race_no_response =  total-sum_of_all_races, and report out if a 'race_no_response' doesn't already exist
----------------------------------------------------------
, sum_of_all_races as ( -- 1. compute sum of all races for each score 1-5 
    select 
        source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
       --demographic_category,
       --'sum-of-all-races' as demographic_group,
        score_category,
        score_of,
        sum(num_students) as num_students
    from agg_exam_results
    where 
        demographic_category='race' 
        and demographic_group <> 'race_no_response' --we want to compute the sum of all reported races; some data sets have 'race_no_response' already. even though we compute it here, we will exclude it later if already existed.
    {{dbt_utils.group_by(8)}}
)
, totals_only as ( -- 2. find total num students for each score 1-5 
    select 
        t.source,
        t.exam_year,
        t.pd_year,
        t.reporting_group,
        t.rp_id,
        t.exam,
        --t.demographic_category,
        --'totals only' as demographic_group,
        t.score_category,
        score_of,
        sum(num_students) as num_students
    from agg_exam_results t
    where demographic_category='total'
    {{dbt_utils.group_by(8)}}
)
, existing_race_no_response as ( --3. find all exam_year|reporting_group|exam that already have 'race_no_response' included
    select
        source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
        'race' as demographic_category,
        'race_no_response' as demographic_group,
        score_category,
        score_of,
        num_students
    from agg_exam_results
    where demographic_category = 'race' and demographic_group = 'race_no_response'
)
, race_no_response_calc as ( -- 4. compute race_no_response as diff between total and sum_of_all_races. Only use the computed result if the original dataset doesn't have an existing race_no_response field.
    select * 
    from ( 
        select 
            'calculated' as source,
            t.exam_year,
            t.pd_year,
            t.reporting_group,
            t.rp_id,
            t.exam,
            'race' as demographic_category,
            'race_no_response' as demographic_group, 
            t.score_category,
            t.score_of,
            --t.num_students as total_num_students,
            --r.num_students as sum_of_races_students,
            (t.num_students - coalesce(r.num_students,0)) as num_students_diff_tot_and_sum
        from totals_only t
        left join sum_of_all_races r
            on t.source = r.source
            and t.exam_year = r.exam_year
            and t.exam = r.exam
            and (t.pd_year = r.pd_year OR (t.pd_year IS NULL and r.pd_year is null))
            and t.reporting_group = r.reporting_group
            and (t.rp_id = r.rp_id OR (t.rp_id IS NULL and r.rp_id is null))
            and t.score_category = t.score_category
            and (t.score_of = r.score_of OR (t.score_of is null and r.score_of is null))
    )
    where not exists ( -- exclude the computed 'race_no_response' IF that field already exists in the orginal data.
        select 1 
        from agg_exam_results ar 
        where ar.exam_year = exam_year
        and ar.exam = exam
        and (ar.pd_year = pd_year OR (ar.pd_year IS NULL and pd_year is null))
        and ar.reporting_group = reporting_group
        and (ar.rp_id = rp_id OR (ar.rp_id IS NULL and rp_id is null))
        and ar.score_category = score_category
        and (ar.score_of = score_of OR (ar.score_of is null and score_of is null))
        and ar.demographic_category = 'race'
        and ar.demographic_group = 'race_no_response'
    )
)
, final as (

    select *
    from bhnapi

    union all

    select *
    from wh_as_other

    union all

    select *
    from race_no_response_calc

    union all

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
        num_students_diff_tot_and_sum
    from race_no_response_calc
)
select * 
from final

