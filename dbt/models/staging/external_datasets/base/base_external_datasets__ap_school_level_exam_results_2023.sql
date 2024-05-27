/* 
    This should be just a straight load of the data from source, and should be a 1:1 - CB dataset : dashboard table
    TBD: do we want to continue to use dashboard.analysis for the COPY from S3, or do we want to make a space/schema for it in dev?

    TODO: rename the source tables according to naming convention
*/ 

with ap_data AS (
    select * 
    from {{source('dashboard_analysis','stg_ap_school_level_exam_results_raw_2023')}}
)
select * 
from ap_data