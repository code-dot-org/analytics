with 
source as (
    select * 
    from {{ source('pegasus_pii', 'storage_apps') }}
)

select * 
from source