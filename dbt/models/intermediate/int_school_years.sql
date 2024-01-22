with 
school_years as (
    select *
    from {{ ref('seed_school_years') }}
)


select *
from school_years
where current_date >= start_date
and current_date <= end_date
