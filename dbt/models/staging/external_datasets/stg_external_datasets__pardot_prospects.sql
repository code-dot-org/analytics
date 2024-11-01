with 
source as (
    select * 
    from {{ ref('seed_pardot_prospects') }}
)

select * 
from source