with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_international_opt_ins')}}
),

renamed as (
    select 
        id as application_id,
        user_id,
        form_data,
        created_at,
        updated_at
    from source 
)

select *
from renamed