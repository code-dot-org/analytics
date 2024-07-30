with 
cs_ambassadors as (
    select *
    from {{ ref('seed_cs_ambassador_app') }}
)

select 
    *
from cs_ambassadors