with 
source as (
    select * 
    from {{ source('dashboard_pii', 'users') }}
    where user_type is not null 
)

select *
from source
