-- This JINJA worked I think but was really slow.  Defered to composing in CTEs below with unions, joins, and multiplication
-- {% set n = 15000 %} -- Set your desired range here
-- with numbers_large as (
-- {% for i in range(n) %}
--     select {{ i }} as num
--     {% if not loop.last %}union all{% endif %}
-- {% endfor %}
-- )

with numbers_small as (
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
    order by 1
)
, date_range as (
    select
        sy.school_year,
        '2013-07-01'::date + num as date,
        date_part(week, date) as iso_week


    from numbers_large
    left join
        {{ ref('seed_school_years') }} as sy
        on
            '2013-07-01'::date
            + num between sy.started_at::date and sy.ended_at::date
    where '2013-07-01'::date + num <= '2030-06-30'::date
),

flagged_week_changes as (
    select
        dr.*,
        lag(iso_week) over (order by date) as prev_iso_week,

        -- 'flag' dates where an iso week changes, or the school year (july 1) changes
        case
            when
                iso_week != prev_iso_week
                or (date_part(month, date) = 7 and date_part(day, date) = 1)
                then 'flag'
        end as week_change
    from date_range dr
    order by dr.date
),

school_week_calc as (
    select
        *,
        -- the school year week is the sum of all the "flagged" iso_week changes, including july 1, partition by school year so counting starts over each july 1
        sum(case when week_change = 'flag' then 1 else 0 end)
            over (
                partition by school_year
                order by date rows between unbounded preceding and current row
            )

        as school_year_week
    from flagged_week_changes
    order by date
)

select

    school_year,
    iso_week,
    school_year_week,
    min(date) as started_at,
    max(date) as ended_at,
    (ended_at - started_at) + 1 as days_interval
from school_week_calc
group by
    school_year,
    iso_week,
    school_year_week

order by started_at
