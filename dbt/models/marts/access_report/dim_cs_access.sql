with 

review_table as (
    select * 
    from {{ ref('stg_external_datasets__access_report_review_table') }}
)

select * 
from review_table