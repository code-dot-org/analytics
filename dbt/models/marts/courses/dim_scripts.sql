with 
scripts as (
    select * 
    from {{ ref('stg_dashboard__scripts') }}
)

select * 
from scripts