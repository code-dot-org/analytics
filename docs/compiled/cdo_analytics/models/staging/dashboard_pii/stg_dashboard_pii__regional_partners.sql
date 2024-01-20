with  __dbt__cte__base_dashboard_pii__regional_partners as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."regional_partners"
    where not deleted_at
),

renamed as (
    select
        id                  as regional_partner_id,
        "group"             as regional_partner_group,
        name                as regional_partner_name,
        urban               as is_urban,
        attention,
        street,
        apartment_or_suite,
        city,
        state,
        zip_code,
        -- phone_number,
        -- notes,
        created_at,
        updated_at,
        -- properties,
        is_active
    from source
)

select * 
from renamed
), regional_partners as (
    select * 
    from __dbt__cte__base_dashboard_pii__regional_partners
)

select * 
from regional_partners