/*
    Produces the most recent aicode/nces mappings we have.
*/

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
    --rn,
    ai_code,
    nces_id,
    school_name,
    city,
    state,
    zip,
    exam_year as most_recent_update_year,
    source
FROM
    ranked_most_recent_record
WHERE
     rn = 1