with exam_results as (
    select *
    from {{ ref('int_ap_school_level_exam_results') }}
)
--select * from exam_results where score_of IS NOT NULL

select 
    exam_year,
    ai_code,
    high_school_name,
    state,
    exam, 
    demographic_category,
    demographic_group,
    max(num_schools) as num_schools, --every school+score+group combo has the number of schools that apply.  Max will be 1 for individual schools and NN for the "less than 10 aggregate"
    --SUM(CASE WHEN score_category = 'total' THEN num_students ELSE 0 END) AS total_students, --used to sanity check. scores 1-5 should = total
    SUM(CASE WHEN score_of IN (1,2,3,4,5) THEN num_students ELSE 0 END) AS num_taking,
    SUM(CASE WHEN score_of IN (3,4,5) THEN num_students ELSE 0 END) AS num_passing,
    COALESCE(num_passing::float / NULLIF(num_taking::float, 0), 0) AS pct_passing --prevent division by 0
from
    exam_results
group by
    exam_year,
    ai_code,
    high_school_name,
    state,
    exam, demographic_group,
    demographic_category
order by
    exam_year, ai_code
