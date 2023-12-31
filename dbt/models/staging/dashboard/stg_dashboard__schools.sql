with schools as (
    select *
    from {{ ref('base_dashboard__schools') }}
),

final as (
    select 
        case when len(school_id) = 11 
             then lpad(school_id,12,'0') -- (js) adjustment to compensate for application dropping leading zeroes
        else school_id 
        end as school_id,
        school_district_id,
        school_name,
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
        last_known_school_year_open
from schools
)

select *
from final 