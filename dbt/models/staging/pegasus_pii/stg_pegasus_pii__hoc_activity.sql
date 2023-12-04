{{
    config(
        materialized='incremental',
        unique_key='hoc_activity_id'
    )
}}

with
hoc_activity as (
    select
        hoc_activity_id,
        referer,
        company,
        tutorial,
        coalesce(started_at, pixel_started_at)                      as started_at,
        case when pixel_started_at is not null then 1 else 0 end    as is_third_party,
        country_code,
        state_code,
        city,
        country,
        state
    from {{ ref("base_pegasus_pii__hoc_activity") }}
    {% if is_incremental() %}

    where started_at > (select max(started_at) from {{ this }} )
    
    {% endif %}
)

select *
from hoc_activity

