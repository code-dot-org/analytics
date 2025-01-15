with 

results_2022_2023 as (
   select 
        exam_year
        , country
        , ai_code
        , high_school_name
        , state
        , exam
        , demographic_group
        , demographic_category
        , score_category
        , score_of
        , num_schools
        , num_students
   from {{ ref('stg_external_datasets__ap_school_level_exam_results') }} 
),

results_2024 as (
    select
        exam_year
        , country
        , ai_code
        , high_school_name
        , state
        , exam
        , demographic_group
        , demographic_category
        , score_category
        , score_of
        , num_schools
        , num_students
    from {{ref('stg_external_datasets__ap_school_level_exam_results_2024') }} 
), 

unioned as
(
    select * from 
    results_2024
    union 
    select * from 
    results_2022_2023
)

select *
from unioned

