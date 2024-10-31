with megatable AS (
    select * from {{ source('external_datasets','access_report_megatable_2024') }}
)
select 
    * 
from megatable