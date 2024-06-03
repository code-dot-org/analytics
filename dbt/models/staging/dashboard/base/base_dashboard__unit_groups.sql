with 
source as (
    select * 
    from {{ source('dashboard', 'unit_groups') }}
),

renamed as (
    select
        id as unit_group_id,
        name as unit_group_name,
        properties,
        created_at,
        updated_at,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience
    from source
)

select * 
from renamed