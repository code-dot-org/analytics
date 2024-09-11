
with 
school_status as (
    select * 
    from {{ ref('dim_school_status') }}
),

dim_schools as (
    select * 
    from {{ ref('dim_schools') }}
),

school_status_sy as (
    select 
        school_status.school_id,
        school_status.school_year,
        school_status.status,
        school_status.active_courses,
        dim_schools.school_level_simple,
        dim_schools.is_stage_el,
        dim_schools.is_stage_mi,
        dim_schools.is_stage_hi,
        school_status.school_started_at
    from school_status 
    left join dim_schools 
        on school_status.school_id = dim_schools.school_id
)
, school_weeks as (
    select * FROM {{ref('int_school_weeks')}}
)

, active_schools_by_week as (
    select 
        sssy.school_year,
        sssy.school_level_simple,
        sssy.is_stage_el,
        sssy.is_stage_mi,
        sssy.is_stage_hi,
        sssy.status,
        sw.iso_week start_week, --keeping start_week alias for now, even though the output gets re-aliased as iso_week
        sw.school_year_week,
        sw.started_at week_of,
        count(distinct school_id) as num_schools
    from school_status_sy sssy
    left join school_weeks sw
        on school_started_at 
            between sw.started_at and sw.ended_at
    where status like 'active %'
    group by 1,2,3,4,5,6,7,8,9

)
, running_totals_by_week as (
    select
        school_year,
        status,
        start_week,
        school_year_week,
        min(week_of)::date week_of,
        sum(case when is_stage_el=1 then num_schools else 0 end) as el_schools,
        sum(case when is_stage_mi=1 then num_schools else 0 end) as mi_schools,
        sum(case when is_stage_hi=1 then num_schools else 0 end) as hi_schools,
        sum(el_schools) over (
            partition by school_year, status order by school_year_week
            rows between unbounded preceding and current row
        ) el_running_total,
        
        sum(mi_schools) over (
            partition by school_year, status order by school_year_week
            rows between unbounded preceding and current row
        ) mi_running_total,
        
        sum(hi_schools) over (
            partition by school_year, status order by school_year_week
            rows between unbounded preceding and current row
        ) hi_running_total
    from active_schools_by_week
    group by 1,2,3,4
    order by status, school_year_week
),

report_by_week as (
    select
        'elementary'                as school_level,
        school_year,
        status,
        start_week                  as iso_week,
        school_year_week,
        week_of,
        el_schools                  as num_schools_this_week,
        el_running_total            as num_schools_running_total
    
    from running_totals_by_week

  
    union all
  
    select
        'middle',
        school_year,
        status,
        start_week,
        school_year_week,
        week_of,
        mi_schools,
        mi_running_total
    from running_totals_by_week

    union all
  
    select
        'high',
        school_year,
        status,
        start_week,
        school_year_week,
        week_of,
        hi_schools,
        hi_running_total
    from running_totals_by_week
),

final as (
    select 
        school_level,
        school_year, 
        status, 
        iso_week,
        school_year_week,
        week_of, 
        coalesce(num_schools_this_week,0)       as num_schools_this_week,
        coalesce(num_schools_running_total,0)   as num_schools_running_total
    from report_by_week )

select * 
from final 
--where num_schools_this_week is null 