with megatable AS (
    select * from {{ source('external_datasets','access_report_review_table_2023') }}
)
select 
    * 
from megatable