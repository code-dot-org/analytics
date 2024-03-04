with 
source as (
    select * 
    from {{ source('dashboard_pii','potential_teachers') }}
),

renamed as (
    select
        id as potential_teacher_id,
        {# name as potential_teacher_name,
        email as potential_teacher_email, #}
        script_id,
        case when receives_marketing = 1 then 1 else 0 end as is_receiving_marketing,
        created_at,
        updated_at
    from source 
)

select * 
from source