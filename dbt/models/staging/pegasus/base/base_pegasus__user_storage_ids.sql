with 
source as (
    select * 
    from {{ source('pegasus', 'user_storage_ids') }}
)

select * 
from source
  