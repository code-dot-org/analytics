/* 
    The table public.stg_2017_2022_agg_ap_exam_results_raw was constructed
    by just exporting analysis.ap_exam_results_raw to a csv and putting that in 
    S3.  Then this table was constructed in dev.public and loaded via call to 
    COPY public.stg_2017_2022_agg_ap_exam_results_raw

    FROM 's3://cdo-data-sharing-internal/ap_data/aggregated_data/2017_2022_agg_ap_exam_results_raw.csv'
    IAM_ROLE 'arn:aws:iam:****************' 
    FORMAT AS CSV IGNOREHEADER 1;


    NOTE: if aggregate national data is sent separately it's assumed that any records for it
    will be added to the source data -- at least as of this time.  

    OPEN QUESTION: whether aggregate ap exam results should be split csp and csa like the school_level is.
    These aggregate reports are relatively short these days (~10 records) so it seems like it's not too hard
    to merge them together in the source csv/table we end up using. If national comes in a separate
    report though, that's annoying.
*/ 

with ap_data AS (
    select * from {{ source('dashboard_analysis','stg_agg_ap_exam_results_raw_2023') }}
)
select * from ap_data