with exam_results as (
    select *
    from {{ ref('int_ap_agg_exam_results') }}
)
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
