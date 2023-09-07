with 
levels as (
    select * from {{ ref('base_dashboard__levels')}}
)

select * from levels