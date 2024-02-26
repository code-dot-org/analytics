with recursive numbers as (
    select 0 as num
    union all
    select num + 1 from numbers where num < 9999
),

calendar as (
    select
        isy.school_year,
        isy.started_at::date + num as date,
        date_part('week', isy.started_at::date + num)::integer as iso_week
    from int_school_years isy
    join numbers n on isy.started_at::date + num <= isy.ended_at::date
),

final as (
    select
        school_year,
        iso_week,
        row_number() over (partition by school_year order by date) as school_year_week,
        min(date) as start_dt,
        (max(date + '1 day'::interval) - '1 second'::interval)::timestamp as end_dt,
        count(*) as days_interval
    from calendar
    group by
        school_year,
        iso_week)

select *
from final