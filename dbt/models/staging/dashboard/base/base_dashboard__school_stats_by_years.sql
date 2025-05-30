with 
source as (
    select * 
    from {{ source('dashboard', 'school_stats_by_years') }}
)

select * 
from source