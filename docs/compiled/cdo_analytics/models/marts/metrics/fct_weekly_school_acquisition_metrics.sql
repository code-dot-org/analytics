with 
school_status as (
    select * 
    from "dev"."dbt_jordan"."dim_school_status"
),

dim_schools as (
    select * 
    from "dev"."dbt_jordan"."dim_schools"
),

school_status_sy as (
    select 
        school_status.school_id,
        school_status.school_year,
        school_status.status,
        school_status.active_courses,
        dim_schools.school_level_simple,
        school_status.school_started_at
    from school_status 
    left join dim_schools 
        on school_status.school_id = dim_schools.school_id
),

active_schools_by_week as (
    select 
        school_year,
        school_level_simple,
        status,
        date_part(week, school_started_at) as start_week,
        (start_week + 26) % 52 as sy_week_order, -- hardcoded for now, make dynamic later
        min(school_started_at) as week_of,
        count(distinct school_id) as num_schools
    from school_status_sy
    where status like 'active %'
    group by 1,2,3,4,5

),

running_totals_by_week as (
    select
        school_year,
        status,
        start_week,
        sy_week_order,
        min(week_of)::date week_of,
        sum(case when school_level_simple like '%el%' then num_schools else 0 end) as el_schools,
        sum(case when school_level_simple like '%mi%' then num_schools else 0 end) as mi_schools,
        sum(case when school_level_simple like '%hi%' then num_schools else 0 end) as hi_schools,
        sum(el_schools) over (
            partition by school_year, status order by sy_week_order
            rows between unbounded preceding and current row
        ) el_running_total,
        
        sum(mi_schools) over (
            partition by school_year, status order by sy_week_order
            rows between unbounded preceding and current row
        ) mi_running_total,
        
        sum(hi_schools) over (
            partition by school_year, status order by sy_week_order
            rows between unbounded preceding and current row
        ) hi_running_total
    from active_schools_by_week
    group by 1,2,3,4
    order by status, sy_week_order
),

report_by_week as (
    select
        'elementary' as school_level,
        school_year,
        status,
        start_week,
        sy_week_order,
        week_of,
        el_schools as num_schools_this_week,
        el_running_total as num_schools_running_total
    
    from running_totals_by_week

  
    union all
  
    select
        'middle' as school_level,
        school_year,
        status,
        start_week,
        sy_week_order,
        week_of,
        mi_schools as num_schools_this_week,
        mi_running_total as num_schools_running_total
    from running_totals_by_week

    union all
  
    select
        'high' as school_level,
        school_year,
        status,
        start_week,
        sy_week_order,
        week_of,
        hi_schools as num_schools_this_week,
        hi_running_total as num_schools_running_total
    from running_totals_by_week
)

select * 
from report_by_week