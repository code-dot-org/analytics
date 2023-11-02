with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_workshops_facilitators') }}
),

renamed as (
    select
        pd_workshop_id,
        user_id
    from source
)

select * 
from renamed