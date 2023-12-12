

/* 
    This is my solution to the problem of where to store data data imported via .csv that's too big for dbt seed.
    1. store a file in S3 with the name e.g. seed_2022_school_level_ap_exam_results.csv
    2. create a table in dev.public with a parellel name: public.seed_2022_school_level_ap_exam_results (with column names and types that match the csv)
    3. do an s3 COPY command like so:
        COPY public.seed_2022_school_level_ap_exam_results
        FROM 's3://cdo-data-sharing-internal/ap_data/school_level_data/seed_2022_school_level_ap_exam_results.csv'
        IAM_ROLE 'arn:aws:iam::475661607190:role/redshift-s3' 
        FORMAT AS CSV IGNOREHEADER 1;
    4. make stg__ view to reference in subsequent models.

    NOTE: It's at this time whether this is right approach to get the the raw data for cases like this.
*/ 

with ap_data AS (
    SELECT * FROM public.seed_2022_school_level_ap_exam_results
)
SELECT * FROM ap_data