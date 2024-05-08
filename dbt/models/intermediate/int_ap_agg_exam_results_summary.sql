with exam_results as (
    select * from {{ ref('int_ap_agg_exam_results') }}
    --union all
    --
)
, all_summary as (
    select
        exam_year,
        exam_group,
        rp_id,
        exam,
        demographic_group,
        demographic_category,
        -- SUM(CASE WHEN score_category = 'total' THEN num_students ELSE 0 END) AS total_students, --used to sanity check. scores 1-5 should = total
        SUM(CASE WHEN score_of IN (1,2,3,4,5) THEN num_students ELSE 0 END) AS num_taking,
        SUM(CASE WHEN score_of IN (3,4,5) THEN num_students ELSE 0 END) AS num_passing,
        COALESCE(num_passing::float / NULLIF(num_taking::float, 0), 0) AS pct_passing --prevent division by 0
    from
        exam_results
    group by
        exam_year,
        exam_group,
        rp_id,
        exam,
        demographic_group,
        demographic_category
    order by
        exam_year
)
, bhnapi as (
    select
        exam_year,
        exam_group,
        rp_id,
        exam,
        'bhnapi' as demographic_group,
        'race-bhnapi' as demographic_category,
        sum(num_taking) as num_taking_computed,
        sum(num_passing) as num_passing_computed,
        COALESCE(num_passing_computed::float / NULLIF(num_taking_computed::float, 0), 0) AS pct_passing -- need to recomput from new sums
    from all_summary
    where demographic_group IN ('black','hispanic','american_indian','hawaiian')
    {{ dbt_utils.group_by(6) }}  
)
, single_race as (
     select
        exam_year,
        exam_group,
        rp_id,
        exam,
        'single_race' as demographic_group,
        demographic_category,
        sum(num_taking) as num_taking,
        sum(num_passing) as num_passing,
        COALESCE(sum(num_passing)::float / NULLIF(sum(num_taking)::float, 0), 0) AS pct_passing -- need to recomput from new sums
    from all_summary
    where demographic_group IN ('black','hispanic','american_indian','hawaiian','white','asian','other_race')
    {{ dbt_utils.group_by(6) }}  
)
-- , tr_urg as (
--       select
--         exam_year,
--         exam_group,
--         rp_id,
--         exam,
--         'tr_urg' as demographic_group,
--         demographic_category,
--         sum(num_taking)::float*0.6 as num_taking_computed,
--         sum(num_passing)::float*0.6 as num_passing_computed,
--         COALESCE(num_passing_computed::float / NULLIF(num_taking_computed::float, 0), 0) AS pct_passing_computed -- need to recomput from new sums
--     from all_summary
--     where demographic_group IN ('two_or_more')
--     {{ dbt_utils.group_by(6) }}  
-- )
-- , tr_non_urg as (
--       select
--         exam_year,
--         exam_group,
--         rp_id,
--         exam,
--         'tr_non_urg' as demographic_group,
--         demographic_category,
--         sum(num_taking)::float*0.4 as num_taking_computed,
--         sum(num_passing)::float*0.4 as num_passing_computed,
--         COALESCE(num_passing_computed::float / NULLIF(num_taking_computed::float, 0), 0) AS pct_passing_computed -- need to recomput from new sums
--     from all_summary
--     where demographic_group IN ('two_or_more')
--     {{ dbt_utils.group_by(6) }}  

-- )
-- , urg as (
--     select
--         b.exam_year,
--         b.exam_group,
--         b.rp_id,
--         b.exam,
--         'urg' as demographic_group,
--         b.demographic_category,
--         (b.num_taking_computed + t.num_taking_computed) as num_taking_computed2,
--         (b.num_passing_computed + t.num_passing_computed) as num_passing_computed2,
--         COALESCE(num_passing_computed2::float / NULLIF(num_taking_computed2::float, 0), 0) AS pct_passing_computed -- need to recomput from new sums

--     from bhnapi b
--     left join tr_urg t 
--         ON b.exam_year = t.exam_year 
--         and b.exam_group = t.exam_group 
--         and (b.rp_id = t.rp_id OR (b.rp_id IS NULL or t.rp_id IS NULL))
--         and b.exam = t.exam
--         and b.demographic_category = t.demographic_category


-- )
select * from
(select * from all_summary
union all
select * from bhnapi
union all
select * from single_race
-- union all
-- select * from tr_urg
-- union all
-- select * from tr_non_urg
-- union all
-- select * from urg
)
where exam_year='2023' and exam = 'csp' and exam_group='cdo_audit' and demographic_category='race'
order by demographic_category, demographic_group