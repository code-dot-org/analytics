with ambassador_apps as (
    select * 
    from {{ ref('stg_external_datasets__ambassador_apps') }}
)

select * 
from ambassador_apps