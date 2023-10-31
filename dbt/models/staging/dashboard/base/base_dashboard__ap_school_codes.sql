with 
source as (
    select * 
    from {{ source('dashboard', 'ap_school_codes') }}
),

renamed as (
    select
        school_id,
        school_year,
        school_code,
        created_at,
        updated_at
    from source
)

select * 
from renamed