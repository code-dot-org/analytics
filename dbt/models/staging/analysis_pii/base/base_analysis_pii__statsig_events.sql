with 
source as (
    select * 
    from {{ source('analysis_pii', 'statsig_events') }}
)

select * 
from source