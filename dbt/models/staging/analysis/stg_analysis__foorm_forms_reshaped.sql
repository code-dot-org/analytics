with 

foorm_forms_reshaped as (
    select *
    from {{ ref('base_analysis__foorm_forms_reshaped') }} 
)

select * 
from foorm_forms_reshaped

 