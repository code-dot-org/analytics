with 
source as (
    select * 
    from {{ source('dashboard', 'schools') }}
)

select * 
from source