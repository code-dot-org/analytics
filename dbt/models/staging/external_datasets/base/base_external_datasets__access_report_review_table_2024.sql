with review_table AS (
    select * from {{ source('external_datasets','access_report_review_table_2024') }}
)
select 
    * 
from review_table