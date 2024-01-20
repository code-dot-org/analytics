

with
 __dbt__cte__base_pegasus_pii__hoc_activity as (
with 
source as (
    select * 
    from "dashboard"."pegasus_pii"."hoc_activity"
),

renamed as (
    select
        id                                      as hoc_activity_id,
        referer,
        company,
        tutorial,
        started_at,
        pixel_started_at,
        country_code,
        state_code,
        city,
        country,
        state
    from source
)

select * 
from renamed
), hoc_activity as (
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
    from __dbt__cte__base_pegasus_pii__hoc_activity
    

    where started_at > (select max(started_at) from "dev"."dbt_jordan"."stg_pegasus_pii__hoc_activity" )
    
    
)

select *
from hoc_activity