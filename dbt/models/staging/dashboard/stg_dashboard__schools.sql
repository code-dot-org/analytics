with schools as (
    select *
    from {{ ref('base_dashboard__schools') }}
)

, final as (
    select 
        {{ pad_school_id('id') }}  as school_id,
        school_district_id,
        lower(name) as school_name,
        lower(city) as city,
        upper(state) as state,
        zip,
        school_type,
        created_at,
        updated_at,
        lower(address_line1) as address_line1,
        lower(address_line2) as address_line2,
        lower(address_line3) as address_line3,
        latitude,
        longitude,
        school_category,
        last_known_school_year_open,
        county_id,
        lower(county_name) as county_name
    from schools
)

select * 
from final