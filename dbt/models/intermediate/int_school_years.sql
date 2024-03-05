with 
school_years as (
    select
        school_year::varchar,
        school_year_int::integer,
        started_at::timestamp,
        ended_at::timestamp,
        school_year_long::varchar
    from {{ ref('seed_school_years') }}
)


select *
from school_years
where current_date >= started_at