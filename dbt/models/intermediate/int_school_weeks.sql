with 
numbers_small as (
    select 0 as num
    union all
    select 1
    union all
    select 2
    union all
    select 3
    union all
    select 4
    union all
    select 5
    union all
    select 6
    union all
    select 7
    union all
    select 8
    union all
    select 9
),

numbers_large as (
    select a.num + 10 * b.num + 100 * c.num + 1000 * d.num + 10000 * e.num as num
    from numbers_small as a
    cross join numbers_small as b
    cross join numbers_small as c
    cross join numbers_small as d
    cross join numbers_small as e
),

school_years as (
    select *
    from {{ ref('seed_school_years') }}
),

date_range as (
    select
        sy.school_year,
        dateadd(dd,num,'2013-07-01') as date_start,
        date_part(week, start_date) as iso_week
    from numbers_large
    left join school_years sy 
        on dateadd(dd,num,'2013-07-01')  
            between sy.start_date and sy.end_date
),

flagged_week_changes as (
    select
        school_year,
        date_start,
        iso_week,
        lag(iso_week) over (order by date_start) as prev_iso_week,

        -- 'flag' dates where an iso week changes, or the school year changes (july 1)
        case
            when
                iso_week != prev_iso_week
                or (date_part(month, date_start) = 7 and date_part(day, date_start) = 1)
            then 'flag'
        end as week_change
    from date_range dr
),

school_week_calc as (
    select 
        school_year,
        date_start,
        iso_week,
        week_change,
        sum(
            case 
                when week_change = 'flag' 
                then 1 
                else 0 
            end)
            over (
                partition by school_year
                order by date_start rows between unbounded preceding and current row
            ) as school_year_week
    from flagged_week_changes
    {{ dbt_utils.group_by(4) }}
    ),

final as (
    select
        school_year,
        iso_week,
        school_year_week,
        date_start,
        dateadd(s, -1, max(date_start)+1) as date_end
        {# datediff('day',start_date,end_date)+1 as days_interval #}
    from school_week_calc
)

select * 
from final 