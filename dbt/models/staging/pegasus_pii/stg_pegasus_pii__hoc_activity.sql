/*{{
    config(
        materialized='incremental',
        unique_key='hoc_start_id'
    )
}}*/

with internal_tutorial_list as (
    select * from {{ref('seed_hoc_internal_tutorials')}}
)

, hoc_starts as (
    select * from {{ ref("base_pegasus_pii__hoc_activity") }}
)

, combined as (
    select * from hoc_starts
    left join internal_tutorial_list on hoc_starts.tutorial = internal_tutorial_list.tutorial_codes
)

, final as (
    select
        hoc_start_id,
        referer,
        company,
        tutorial,
        coalesce(started_at, pixel_started_at, pixel_finished_at)   as started_at,
        case
            when combined.is_internal is null then 1
            else 0
        end as is_third_party,
        country_code,
        state_code,
        city,
        country,
        state
    from combined
   /* {% if is_incremental() %}
        where coalesce(started_at, pixel_started_at, pixel_finished_at) > (select max(started_at) from {{ this }} )
    {% endif %}*/
)

select *
from final
