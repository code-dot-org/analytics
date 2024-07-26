/*
    Produces the most recent aicode/nces mappings we have.
*/

with all_crosswalks as (
    select *
    from {{ ref('stg_external_datasets__ap_crosswalks') }}
),

ranked_most_recent_record as (
    select
        *
        , row_number() over (partition by ai_code order by exam_year desc)                  as rn
    from
        all_crosswalks
)

select
    --rn,
    ai_code,
    nces_id,
    school_name,
    city,
    state,
    zip,
    exam_year                                                                               as most_recent_update_year,
    source
FROM
    ranked_most_recent_record
WHERE
     rn = 1