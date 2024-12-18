with 
free_responses as (
    select * 
    from {{ ref('base_dashboard__level_sources_free_responses') }} )

select * 
from free_responses