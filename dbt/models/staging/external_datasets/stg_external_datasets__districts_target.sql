with 
districts_target as (
    select *
    from {{ ref('seed_districts_target') }}
)

, current_sy as (
    select max(school_year) as school_year
    from {{ref('int_school_years')}}
)

select 
    district_id,
    school_year
from districts_target
cross join current_sy
