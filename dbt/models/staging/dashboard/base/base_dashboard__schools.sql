with 
source as (
      select * from {{ source('dashboard', 'schools') }}
),

renamed as (
    select
        id                  as school_id,
        school_district_id,
        name                as school_name,
        city,
        state,
        zip,
        school_type,
        created_at,
        updated_at,
        address_line1,
        address_line2,
        address_line3,
        latitude,
        longitude,
        school_category,
        state_school_id, -- (js) recently removed (see: https://github.com/code-dot-org/code-dot-org/pull/53871#event-10428323040)
        last_known_school_year_open
    from source
)

select * from renamed