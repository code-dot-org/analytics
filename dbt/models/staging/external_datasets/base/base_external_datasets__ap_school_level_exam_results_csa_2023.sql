/* 
    This should be just a straight load of the data from source, and should be a 1:1 - CB dataset : dashboard table
    TBD: do we want to continue to use dashboard.analysis for the COPY from S3, or do we want to make a space/schema for it in dev?

    TODO: rename the source tables according to naming convention
*/ 

with ap_data AS (
    SELECT * FROM {{source('dashboard_analysis','stg_ap_exam_results_school_level_2023_csa')}}
)
SELECT * FROM ap_data