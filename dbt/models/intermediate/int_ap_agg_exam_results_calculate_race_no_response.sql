/*
    This model computes the number of students in each data set who did not self-identify their race or ethnicity i.e. the "race_no_response" group.

    The TRICKY part is that sometimes the College Board includes the "race_no_response" group and sometimes they don't - we need keep the data
    if they provide it, or compute it if they don't.

    This model goes through some pains to preserve the orginal "race_no_response" value IF IT EXISTS and otherwise
    substitute a calculated version. If the original is preserved then source = 'college board', otherwise source='computed' 
    
    The ultimate goal: we want to ensure that every year|reporting|group|exam 
    has a value for demographic_group='race_no_response' under the demographic_category='race'
    such that the sum of all values under demographic_category='race' is equal to to the total.

    The code below executes the following steps (more or less):

    1. identify every possible year|exam|group|score that DOES NOT have "race_no_response" demographic group already
    2. compute SUM-OF-ALL-RACES for those ^^^ group (ie. sum(num_taking) where demographic_category='race') 
    3. find TOTAL num students for those ^^^ groups (i.e. where demographic_category = 'total')
    4. compute "race_no_response" as: (TOTAL - SUM-OF-ALL-RACES) and union it to the records.
    5. Output: all agg exam results UNION all "race_no_response" values for records that didn't have it
----------------------------------------------------------
*/

with all_agg_exam_results as (
    select * 
    from {{ ref('int_ap_agg_exam_results_union_agg_school_level') }}
)
-- Step 1: identify every possible year|exam|group|score that DOES NOT have "race_no_response" demographic group already
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
    where 
        demographic_category = 'race' 
        and demographic_group = 'race_no_response'
)
, agg_results_missing_race_no_response_field as (
    select rco.*
    from all_agg_exam_results rco
    left join agg_results_that_have_race_no_response_field grnr -- could be inner select, rather than using a CTE?
        on rco.source = grnr.source
        and rco.exam_year = grnr.exam_year
        and (rco.pd_year = grnr.pd_year or (rco.pd_year is null and grnr.pd_year is null))
        and rco.reporting_group = grnr.reporting_group
        and (rco.rp_id = grnr.rp_id or (rco.rp_id is null and grnr.rp_id is null))
        and rco.exam = grnr.exam
        and rco.score_category = grnr.score_category
    where 
        grnr.source is null  --could be grnr.anything is null 
         and rco.demographic_category='race' 
         and rco.demographic_group <> 'race_no_response' 
)
-- Step 2: compute SUM-OF-ALL-RACES for the agg_results_missing_race_no_response_field set
, sum_of_all_races as ( -- 1. compute sum of all races for each score 1-5 
    select 
        source,
        exam_year,
        pd_year,
        reporting_group,
        rp_id,
        exam,
       --demographic_category,        -- leaving these two lines commented out to prove intention to do so
       --demographic_group,           -- this is a stepping stone/input to the final calc with will be hard-coded for demographic_category and _group
        score_category,
        score_of,
        sum(num_students) as num_students
    from agg_results_missing_race_no_response_field
    -- where 
    --     demographic_category='race' 
    --     and demographic_group <> 'race_no_response' 
    {{dbt_utils.group_by(8)}}
)
-- Step 3: find TOTAL num students for every possible year|exam|group|score
, totals_only as ( 
    select 
        t.source,
        t.exam_year,
        t.pd_year,
        t.reporting_group,
        t.rp_id,
        t.exam,
        --t.demographic_category,        -- leaving these two lines commented out to prove intention to do so
        --t.demographic_group,           -- this is a stepping stone/input to the final calc with will be hard-coded for demographic_category and _group
        t.score_category,
        score_of,
        sum(num_students) as num_students
    from all_agg_exam_results t
    where demographic_category='total'
    {{dbt_utils.group_by(8)}}
)
-- Step 4: compute "race_no_response" as: (TOTAL - SUM-OF-ALL-RACES) and union it to the records
-- giving it demographic_cateogry='race' and demographic_group = 'race_no_response'
, race_no_response_calc as ( 
    select * 
    from ( 
        select 
            'calculated' as source,
            t.exam_year,
            t.pd_year,
            t.reporting_group,
            t.rp_id,
            t.exam,
            'race' as demographic_category,             -- in theory this should be run through the normalization macro
            'race_no_response' as demographic_group,    -- ibid.
            t.score_category,
            t.score_of,
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
-- Step 5
, final as (
    select * from all_agg_exam_results
    union all
    select * from race_no_response_calc
)
select * from final

