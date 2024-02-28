{#
Casting data types to match analysis.school_years

REFERENCE:
> DESCRIBE analysis.school_years
COLUMN_NAME	DATA_TYPE
school_year	varchar(256)
school_year_int	integer
started_at	timestamp
ended_at	timestamp
school_year_long	varchar(256)


#}

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