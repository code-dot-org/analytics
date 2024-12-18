with
source as (
    select * 
    from {{ source('dashboard_pii', 'level_sources') }}
),

renamed as (
    select 
        id as level_sources_id,
        level_id,
        data,
        created_at,
        updated_at
    from source 
)

select * 
from renamed