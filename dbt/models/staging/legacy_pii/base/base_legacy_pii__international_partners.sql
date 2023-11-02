with 
source as (
      select * 
      from {{ source('legacy_pii', 'seed_international_partners') }}
)

select * 
from source