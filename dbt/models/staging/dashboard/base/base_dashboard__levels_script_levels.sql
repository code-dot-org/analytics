with 
source as (
    select * 
    from {{ source('dashboard', 'levels_script_levels') }}
),

renamed as (
    select
        level_id,
        script_level_id
    from source
)

select * 
from renamed