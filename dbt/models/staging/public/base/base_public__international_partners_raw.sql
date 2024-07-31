with 
source as (
    select * 
    from {{ source('public', 'international_partners_raw') }}
)

select * 
from source
  