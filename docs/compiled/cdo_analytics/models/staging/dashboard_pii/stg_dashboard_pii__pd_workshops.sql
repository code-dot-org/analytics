with
 __dbt__cte__base_dashboard_pii__pd_workshops as (
with 
source as (
      select * from "dashboard"."dashboard_production_pii"."pd_workshops"
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
        notes,
        section_id,
        started_at,
        ended_at,
        created_at,
        updated_at,
        processed_at,
        deleted_at,
        regional_partner_id,
        on_map              as is_on_map,
        funded              as is_funded,
        funding_type,
        properties
    from source
)

select * from renamed
), pd_workshops as (
    select * 
    from __dbt__cte__base_dashboard_pii__pd_workshops
)

select * 
from pd_workshops