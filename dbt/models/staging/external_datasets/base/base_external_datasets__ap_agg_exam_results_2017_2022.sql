/* 
    The table public.stg_2017_2022_agg_ap_exam_results_raw was constructed
    by just exporting analysis.ap_exam_results_raw to a csv and putting that in 
    S3.  Then this table was constructed in dev.public and loaded via call to 
    COPY public.stg_2017_2022_agg_ap_exam_results_raw

    FROM 's3://cdo-data-sharing-internal/ap_data/aggregated_data/2017_2022_agg_ap_exam_results_raw.csv'
    IAM_ROLE 'arn:aws:iam:****************' 
    FORMAT AS CSV IGNOREHEADER 1;
*/ 

with ap_data AS (
    --SELECT * FROM public.stg_2017_2022_agg_ap_exam_results_raw
    select * from {{ source('dashboard_analysis','stg_ap_agg_exam_results_raw_2017_2022') }}

)
SELECT * FROM ap_data