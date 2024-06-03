with 
source as (
    select * 
    from {{ source('dashboard', 'levels') }}
),

renamed as (
    select
        id                      as level_id,
        user_id,
        game_id,
        name,
        level_num,
        ideal_level_source_id,
        type,
        md5,
        audit_log,
        published               as is_published,
        notes,
        properties,
        created_at,
        updated_at
    from source
)

select * 
from renamed 