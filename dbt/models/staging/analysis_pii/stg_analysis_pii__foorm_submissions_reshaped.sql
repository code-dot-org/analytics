with 

foorm_submissions_reshaped as (
    select *
    from {{ ref('base_analysis_pii__foorm_submissions_reshaped') }} 
),

renamed as (
    select 
        submission_id,
        item_name,
        matrix_item_name,
        response_value,
        lower(response_text) as response_text
from foorm_submissions_reshaped )

select * 
from renamed 

    