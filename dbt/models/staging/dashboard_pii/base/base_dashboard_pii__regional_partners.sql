with 
source as (
      select * from {{ source('dashboard_pii', 'regional_partners') }}
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
        phone_number,
        notes,
        created_at,
        updated_at,
        deleted_at,
        properties,
        is_active
    from source
)

select * from renamed