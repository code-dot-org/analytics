with 
source as (
    select * 
    from {{ source('analysis_pii', 'foorm_submissions_reshaped') }}
)

select * 
from source