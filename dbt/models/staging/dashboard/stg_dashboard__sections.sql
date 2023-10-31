with 
sections as (
    select *
    from {{ ref('base_dashboard__sections') }}
    where deleted_at is null 
)

select * 
from sections