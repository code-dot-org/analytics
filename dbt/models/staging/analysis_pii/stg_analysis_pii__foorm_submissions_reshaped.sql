with 

foorm_submissions_reshaped as (
    select *
    from {{ ref('base_analysis_pii__foorm_submissions_reshaped') }} 
)

select * 
from foorm_submissions_reshaped

 