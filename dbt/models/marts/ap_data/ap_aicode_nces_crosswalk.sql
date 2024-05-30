with all_crosswalks as (
    select *
    from {{ ref('stg_external_datasets__ap_crosswalks') }}
)
, ranked_most_recent_record AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ai_code ORDER BY exam_year DESC) AS rn
    FROM
        all_crosswalks
)
SELECT
    ai_code,
    nces_id,
    school_name,
    city,
    state,
    zip,
    exam_year,
    source
FROM
    ranked_most_recent_record
WHERE
    rn = 1