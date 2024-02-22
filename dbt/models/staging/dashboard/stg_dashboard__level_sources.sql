with 
level_sources as (
    select * 
    from {{ ref('base_dashboard__level_sources')}}
)

select * 
from leve_sources