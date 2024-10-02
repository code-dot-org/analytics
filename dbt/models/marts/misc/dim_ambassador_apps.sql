with ambassador_apps as (
    select * 
        , case when created_dt < '07-18-2024' then 1 else 2 end as batch_num
    from {{ ref('stg_external_datasets__ambassador_apps') }}
)

select * 
from ambassador_apps