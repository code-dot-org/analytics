with 
script_names as (
    select * 
    from {{ ref('seed_script_names') }}
)

select * 
from script_names