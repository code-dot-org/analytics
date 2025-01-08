with participation as ( 
    select * 
    from {{ ref('stg_external_datasets__access_report_participation') }}
)

select * 
from participation

--test