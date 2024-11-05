with cte as (
    select * 
    from {{ ref("fct_hoc_event_registrations") }}
),

calc as (
    select 
        school_year,
        extract(year from created_week) as created_year,
        created_week, 
        country, 
        state, 
        sum(num_registrations) as num_registrations
    from cte 
    where country = 'us' 
    group by 1,2,3,4,5
),

augmented as (
select *, 
    lag(num_registrations) 
        over(
            partition by 
                school_year,
                country, 
                state 
            order by created_year desc) as last_year_num_registrations
from calc 
),

final  as (
    select 
        created_week, 
        school_year, 
        country,
        sum(num_registrations) as this_year, 
        sum(last_year_num_registrations) as last_year
    from augmented
    group by 1,2,3
    order by created_week desc
)

select * from final 