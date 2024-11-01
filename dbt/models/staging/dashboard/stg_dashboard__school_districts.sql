with 
school_districts as (
    select * 
    from {{ ref('base_dashboard__school_districts')}}
),

final as (
    select 
        school_district_id,
        lower(school_district_name) as school_district_name,
        lower(school_district_city) as school_district_city,
        upper(school_district_state)   as school_district_state,
        school_district_zip,
        last_known_school_year_open,
        created_at,
        updated_at
    from school_districts )

select * 
from final 