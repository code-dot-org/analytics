with 
sections as (
    select *
    from {{ ref('base_dashboard__sections') }}
)

select * 
from sections