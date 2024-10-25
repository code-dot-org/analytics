with 

review_table as (
    select 
        access_report_year
        , nces_school_id
        , state
        , school_name
        , grade_levels
        , school_type 
        , case 
            when teaches_cs_final in ('HY', 'Y') then 1 
            else 0 
        end                                                     as teaches_cs
    from {{ ref('stg_external_datasets__access_report_review_table') }}
    
)

select * 
from review_table