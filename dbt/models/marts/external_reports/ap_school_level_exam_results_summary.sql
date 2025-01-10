/*
    Summarizes exam results per school into num_taking, num_passing in order to be apples to apples with the tradational aggregated exam results reports

    NOTE: the URG calculation is NOT applied to these data at this time.
*/
with 

crosswalk as (
   select * 
   from {{ ref('stg_external_datasets__ap_crosswalks') }} 
),

exam_results as (
    select * 
    from {{ ref('stg_external_datasets__ap_school_level_exam_results') }}
)

combined as (
    select 
        exam_results.*,
        crosswalk.school_id
    from exam_results
    left join crosswalk 
        on crosswalk.ai_code = exam_results.ai_code
)

select 
    exam_year,
    ai_code,
    school_id,
    high_school_name,
    state,
    exam, 
    demographic_category,
    demographic_group,
    max(num_schools) as num_schools, --every school+score+group combo has the number of schools that apply.  Max will be 1 for individual schools and NN for the "less than 10 aggregate"
    sum(
        case 
            when score_of in (1,2,3,4,5) then num_students 
            else 0 
        end
    )                                                                               as num_taking,
    sum(
        case 
            when score_of in (3,4,5) then num_students 
            else 0 
        end
    )                                                                               as num_passing,
    coalesce(
        num_passing::float / nullif(
            num_taking::float, 
            0
        ), 
        0
    )                                                                               as pct_passing --prevent division by 0

from combined
{{dbt_utils.group_by(8)}}
order by
    exam_year, 
    ai_code
