with 
source as (
    select * 
    from {{ source('analysis', 'foorm_forms_reshaped') }}
)

select * 
from source