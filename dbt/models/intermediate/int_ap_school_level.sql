with 

results_2022_2023 as (
   select * 
   from {{ ref('stg_external_datasets__ap_school_level_exam_results') }} 
),

results_2024 as (
    select * 
   from {{ ref('stg_external_datasets__ap_school_level_exam_results_2024') }} 
)

select * from 
results_2022_2023
union 
select * from 
results_2024