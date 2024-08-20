with 
source as (
    select * 
    from {{ source('public', 'countries') }}
)

select * 
from source
  