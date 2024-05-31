with 
source as (
    select * 
    from {{ source('dashboard', 'unit_groups') }}
),

renamed as (
    select
        id as unit_group_id,
        name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience,
        {# properties, #}
        created_at,
        updated_at
    from source
)

select * 
from renamed