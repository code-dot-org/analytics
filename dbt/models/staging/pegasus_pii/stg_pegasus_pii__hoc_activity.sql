{{
    config(
        materialized='incremental',
        unique_key='hoc_start_id'
    )
}}

with
hoc_starts as (
    select
        hoc_start_id,
        referer,
        company,
        tutorial,
        coalesce(started_at, pixel_started_at, pixel_finished_at)   as started_at,
        state_code,
        city,
        {{ country_normalization('country') }}  as country,
        state
    from
        {{ ref('base_pegasus_pii__hoc_activity') }}
    {% if is_incremental() %}

    where coalesce(started_at, pixel_started_at, pixel_finished_at) > (select max(started_at) from {{ this }} )
    {% endif %} 
)

select *
from hoc_starts