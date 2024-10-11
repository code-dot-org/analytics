with 
participation as (
    select *
    from {{ ref('seed_access_report_participation') }}
)

select * 
from participation