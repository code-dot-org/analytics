with 
levels as (
    select * 
    from {{ ref('stg_dashboard__levels') }}
)

select * 
from levels