with recursive cte(cur) as (
    select 0 as cur
    union all
    select cur + 1 from cte where cur < 9999
),

school_years as (
    select *, 
        min(start_date) over() as first_start_date, 
        max(end_date) over() as last_end_date
    from {{ ref('int_school_years')}}
),
{# select n as num from t #}

date_range as (
    select
        sy.school_year,
        first_start_date + cur as start_date,
        date_part(week, start_date) as iso_week
    from cte
    left join school_years sy
        on first_start_date + cur 
            between sy.start_date and sy.end_date
    where first_start_date + cur < current_date
),

flagged_week_changes as (
    select
        dr.*,
        lag(iso_week) over (order by start_date) as prev_iso_week,
        case
            when
                iso_week != prev_iso_week
                or (date_part(month, start_date) = 7 and date_part(day, start_date) = 1)
            then 'flag'
        end as week_change
    from date_range dr
),

school_week_calc as (
    select
        *,
        sum(
            case 
                when week_change = 'flag' 
                then 1 
                else 0 
            end)
            over (
                partition by school_year
                order by start_date rows between unbounded preceding and current row
            ) as school_year_week
    from flagged_week_changes
),

final as (
    select
        school_year,
        iso_week,
        school_year_week,
        min(start_date) as start_date,
        dateadd(s, -1, max(start_date)+1) as ended_at
        {# datediff('day',start_date,end_date)+1 as days_interval #}
    from school_week_calc
    group by
        school_year,
        iso_week,
        school_year_week
)

select * 
from final

{# flagged_week_changes as (
    select
        dr.*,
        lag(iso_week) over (order by start_date) as prev_iso_week,
        -- 'flag' dates where an iso week changes, or the school year changes (july 1)
        case 
            when iso_week != prev_iso_week
            or (date_part(month, date) = 7 and date_part(day, date) = 1)
            then 'flag'
        end as week_change
    from date_range dr
), #}