with source as (
      select * from {{ source('legacy', 'seed_international_partners') }}
),
renamed as (
    select
        {{ adapter.quote("country_cd") }},
        {{ adapter.quote("display_name") }},
        {{ adapter.quote("alt_name") }},
        {{ adapter.quote("region") }},
        {{ adapter.quote("partner") }},
        {{ adapter.quote("workshop_organizers") }},
        {{ adapter.quote("partner_type") }},
        {{ adapter.quote("contact_name") }},
        {{ adapter.quote("contact_email") }},
        {{ adapter.quote("partner_id") }},
        {{ adapter.quote("exceptions") }}

    from source
)
select * from renamed
  