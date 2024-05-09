with 
source as (
    select * 
    from {{ source('amplitude', 'events_423027') }}
),
select * 
from source