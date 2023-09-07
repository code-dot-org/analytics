with 
school_years as (
    select * 
    from {{ ref('seed_school_years') }}
)

select * 
from school_years