with 
source as (
    select * 
    from {{ source('dashboard', 'level_sources_free_responses') }}
),

renamed as (
    select
        id              as level_sources_free_responses_id,
        level_id,
        md5,
        data,
        created_at,
        updated_at,
        hidden          as is_hidden
    from source
)

select * 
from renamed