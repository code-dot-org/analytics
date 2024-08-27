with 

districts_enrolled as (
    select *
    from {{ ref('seed_districts_enrolled') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

select 
    de.*
    , sy.school_year                                                as school_year_enrolled 
from districts_enrolled                                             as de
join school_years                                                   as sy
    on de.month_closed between sy.started_at and sy.ended_at