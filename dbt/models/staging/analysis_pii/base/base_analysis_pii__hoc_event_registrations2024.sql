with 
source as (
    select * 
    from {{ source('analysis_pii', 'hoc_event_registrations2024') }}
)

select * 
from source