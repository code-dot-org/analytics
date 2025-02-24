with 
districts_target as (
    select *
    from {{ ref('seed_districts_target') }}
)

select 
    district_id,
    school_year
from districts_target
