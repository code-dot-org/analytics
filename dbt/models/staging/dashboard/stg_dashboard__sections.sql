with 
sections as (
    select *
    from {{ ref('base_dashboard__sections') }}
    where deleted_at is null 
        and created_at > '2019-07-01'

)

select * 
from sections