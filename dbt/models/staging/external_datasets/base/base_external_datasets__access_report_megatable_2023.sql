with megatable AS (
    select * from {{ source('external_datasets','access_report_megatable_2023') }}
)
select 
    * 
from megatable