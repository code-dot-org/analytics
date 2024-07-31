with user_storage_ids as (
    select * 
    from {{ ref ('base_pegasus__user_storage_ids') }}
)

select 
    id                                                  as storage_id
    , user_id
from user_storage_ids