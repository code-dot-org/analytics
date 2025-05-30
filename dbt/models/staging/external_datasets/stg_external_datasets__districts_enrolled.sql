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
    de.district_id
    , date_trunc('month',de.month_closed)                           as month_closed 
    , sy.school_year                                                as school_year_enrolled 
    , tier
from districts_enrolled                                             as de
join school_years                                                   as sy
    on de.month_closed between sy.started_at and sy.ended_at
where district_id is not null
    and month_closed > '2000-01-01' --NULL dates from Excel come through as 1899 
    and tier is not null