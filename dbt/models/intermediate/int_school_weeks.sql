{# NOTES:

This creates a corallary to int_school_years, but for weeks of the school year in SCHOOL YEAR ORDER.

We frequently need to Report metrics on a weekly basis based on timestamps.  We want to do this using
ISO weeks to the greatest extent possible, however, because our school year is defined to start on July 1 and end June 30.
It means that the start of the school year can start either in ISO week 26 or 27 depending on the year.
Furthermore, some years I have 52 weeks and some bleed into a 53rd week.
When reporting you're over a year metrics based on school year, we want to line up the beginning of the year with July 1
But adhere to standard iso week, boundaries as much as possible, since it's likely that other metrics might be grouped the same way.

The SOLUTION. 

1. This table numbers the "school year weeks" with week 1 being possibly a fragment (less than 7 days) of a week that starts with July 1,
and stops at the ISO week boundary that first comes after july 1.

2. Weeks 2 through 51 fall on standard I so weak boundaries.

3. Week 52 (or 53 depending on the year) is another possible fragment of a week running from the ISO week boundary that is closest to June 30,
and running up through June 30.

As a result, for example, the end of one school (june 30) and the start of another (july 1) year may fall entirely within ISO week 27, but in
this table that might be split between "school year week" 52 (being 4 days) and school year week 1 of the next year being 3 days.

The final table includes started_at and ended_at fields for joining to (a la our traditional practice with int_school_years)
And for each school year week it also includes an ISO week reference.  Again, ISO weeks match up 1:1 with school_year weeks (albeit different values)
EXCEPT for the week 52/week 1 boundaries.  

METHOD:
1. Generate all possible dates (literally all the days) and their iso weeks from the beginning of code.org time (~July 1, 2013)
2. Flag all the weeks where either the ISO week changes between days OR the school year changes
3. The "school year week" is the sum/count of all the flags up to that date WITHIN (partitioned by) that school year. 

#}



-- This JINJA worked to generate all the days I think but was really slow.  Defered to composing in CTEs below with unions, joins, and multiplication
-- {% set n = 15000 %} -- Set your desired range here
-- with numbers_large as (
-- {% for i in range(n) %}
--     select {{ i }} as num
--     {% if not loop.last %}union all{% endif %}
-- {% endfor %}
-- )


-- Next 3 CTEs are SQL-y way of generating all the days (dates) from the beginning of code.org
-- time, culminating in the CTE 'date_range'
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

        -- 'flag' dates where an iso week changes, or the school year changes (july 1)
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
