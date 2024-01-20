with 
source as (
    select * 
    from "dashboard"."dashboard_production"."level_sources_multi_types"
),

renamed as (
    select
        id              as level_sources_multi_type_id,
        level_source_id,
        level_id,
        data,
        md5,
        hidden          as is_hidden
    from source
)

select * 
from renamed