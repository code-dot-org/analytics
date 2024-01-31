with 
source as (
    select * 
    from {{ source('dashboard_pii', 'users') }}
    where user_type is not null 
),

renamed as (
    select
        id                          as user_id,
        user_type,
        gender,
        birthday,
        active                      as is_active,
        urm                         as is_urg,
        races,
        created_at,
        updated_at,
        purged_at,
        deleted_at
    from source
)

select * 
from renamed