/* 
    This is a dump of all the raw AP exam data that was ingested into dashboard production for exam years 2017-2022.
    For notes on ingesting aggregate exam results in the future...

    see notes in: base_external_datasets__ap_agg_exam_results_2023.sql
*/ 

with ap_data AS (
    select * from {{ source('external_datasets','stg_ap_agg_exam_results_raw_2017_2022') }}

)
SELECT * FROM ap_data