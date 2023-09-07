with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_international_opt_ins')}}
),

renamed as (
    select 
        id as international_opt_in_id,
        user_id,
        form_data, -- would do well to unpack this in stg model
        created_at,
        updated_at
    from source 
)

select *
from renamed