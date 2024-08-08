with 
source as (
    select * 
    from {{ source('public', 'international_contact_info') }}
)

select * 
from source
  