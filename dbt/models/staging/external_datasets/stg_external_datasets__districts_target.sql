with 
districts_target as (
    select *
    from {{ ref('seed_districts_target') }}
)

select *
    lower(district_name) as district_name,
    state,
    {{ pad_district_code('district_id') }} as district_id,
    school_year
from districts_target
