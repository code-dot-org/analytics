with 
source as (
    select * 
    from {{ source('dashboard_pii', 'pd_workshops') }}
    where deleted_at is null
),

renamed as (
    select
        id                  as pd_workshop_id,
        organizer_id,
        location_name,
        location_address,
        processed_location,
        course,
        subject,
        capacity,
        -- notes,
        section_id,
        started_at,
        ended_at,
        created_at,
        updated_at,
        processed_at,
        regional_partner_id,
        on_map              as is_on_map,
        funded              as is_funded,
        funding_type,
        participant_group_type,
        properties,
        module -- (js) new col added (see: https://github.com/code-dot-org/code-dot-org/pull/53949/files)
    from source
)

select * 
from renamed