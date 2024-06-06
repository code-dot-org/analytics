/*
---------------------------------------------------------- 
-- several CTEs to compute race_no_response
-- 1. compute sum of all races for each score 1-5 
-- 2. find total num students for each score 1-5 
-- 3. find existing dataset with a `race_no_response` field already
-- 4. compute race_no_response =  total-sum_of_all_races, and report out if a 'race_no_response' doesn't already exist
----------------------------------------------------------
*/

with all_agg_exam_results as (

    select * 
    from {{ ref('int_ap_agg_exam_results_calculate_agg_school_level') }}
    
)
, agg_results_that_have_race_no_response_field as (
    select
        distinct
        source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
        score_category
    from all_agg_exam_results
    where demographic_category = 'race' and demographic_group = 'race_no_response'
)
, agg_results_missing_race_no_response_field as (
    select rco.*
    from all_agg_exam_results rco
    left join agg_results_that_have_race_no_response_field grnr -- could be inner select?
        on rco.source = grnr.source
        and rco.exam_year = grnr.exam_year
        and (rco.pd_year = grnr.pd_year or (rco.pd_year is null and grnr.pd_year is null))
        and rco.reporting_group = grnr.reporting_group
        and (rco.rp_id = grnr.rp_id or (rco.rp_id is null and grnr.rp_id is null))
        and rco.exam = grnr.exam
        and rco.score_category = grnr.score_category
    where grnr.source is null
)
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
    from agg_results_missing_race_no_response_field
    where 
        demographic_category='race' 
        --and demographic_group <> 'race_no_response' --we want to compute the sum of all reported races; some data sets have 'race_no_response' already. even though we compute it here, we will exclude it later if already existed.
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
    from all_agg_exam_results t
    where demographic_category='total'
    {{dbt_utils.group_by(8)}}
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
        from sum_of_all_races r
        left join   totals_only t --should this just be a join?
            on t.source = r.source
            and t.exam_year = r.exam_year
            and t.exam = r.exam
            and (t.pd_year = r.pd_year OR (t.pd_year IS NULL and r.pd_year is null))
            and t.reporting_group = r.reporting_group
            and (t.rp_id = r.rp_id OR (t.rp_id IS NULL and r.rp_id is null))
            and t.score_category = t.score_category
            and (t.score_of = r.score_of OR (t.score_of is null and r.score_of is null))
    )
)
, final as (
    select * from all_agg_exam_results
    union all
    select * from race_no_response_calc
)
select * from final

