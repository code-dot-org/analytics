with
source as (
    select * 
    from {{ source('dashboard_pii', 'level_sources') }}
),

renamed as (
    select 
        id as level_source_id,
        level_id,
        data,
        created_at,
        updated_at
    from source 
)

select * 
from renamed