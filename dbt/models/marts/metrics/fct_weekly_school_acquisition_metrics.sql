/*Edit log

-- Cory, May 2025 - replaced school_started_at with school_active_at as the core date that this table is based on
*/


with 
school_status as (
    select * 
    from {{ ref('dim_school_status') }}
),

schools as (
    select * 
    from {{ ref('dim_schools') }}
),

-- school_status_sy as (
--     select 
--         school_status.school_id,
--         school_status.school_year,
--         school_status.status,
--         school_status.active_courses,
--         dim_schools.school_level_simple,
--         dim_schools.is_stage_el,
--         dim_schools.is_stage_mi,
--         dim_schools.is_stage_hi,
--         school_status.school_started_at
--     from dim_schools 
--     left join school_status 
--         on school_status.school_id = dim_schools.school_id
-- )

school_weeks as (
    select * 
    from {{ ref('int_school_weeks') }}
)

, active_schools_by_week as (
    select 
        -- schools 
        schools.school_id,
        schools.school_level_simple,
        schools.is_stage_el,
        schools.is_stage_mi,
        schools.is_stage_hi,

        -- school stats
        sssy.school_year,
        sssy.school_active_at,
        sssy.status,
        sssy.active_courses,
        
        -- dates
        sw.iso_week                 as start_week, 
        sw.school_year_week,
        sw.started_at week_of,

        -- aggs 
        count(distinct schools.school_id)   as num_schools
    
    from schools 
    left join school_status as sssy 
        on schools.school_id = sssy.school_id 

    left join school_weeks  as sw
        on sssy.school_active_at 
            between sw.started_at 
                and sw.ended_at
    
    where left(sssy.status,6) = 'active'
    {{ dbt_utils.group_by(12) }}

)
, running_totals_by_week as (
    select
        school_year,
        status,
        start_week,
        school_year_week,
        min(week_of)::date              as week_of,
        
        sum(case when is_stage_el = 1 
                 then num_schools 
                 else 0 end)            as el_schools,
        
        sum(case when is_stage_mi = 1 
                 then num_schools 
                 else 0 end)            as mi_schools,
        
        sum(case when is_stage_hi = 1 
                 then num_schools 
                 else 0 end)            as hi_schools,
        
        sum(el_schools) over (
            partition by 
                school_year, 
                status 
            order by school_year_week
            rows between unbounded preceding 
                     and current row
        )                                   as el_running_total,
        
        sum(mi_schools) over (
            partition by 
                school_year, 
                status 
            order by school_year_week
            rows between unbounded preceding 
                     and current row
        )                                   as mi_running_total,
        
        sum(hi_schools) over (
            partition by 
                school_year, 
                status 
            order by school_year_week
            rows between unbounded preceding 
                     and current row
        )                                   as hi_running_total
    
    from active_schools_by_week

    {{ dbt_utils.group_by(4) }}
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
        status, 
        school_year, 
        school_year_week,
        iso_week,
        week_of, 
        coalesce(num_schools_this_week,0)       as num_schools_this_week,
        coalesce(num_schools_running_total,0)   as num_schools_running_total
    from report_by_week )

select *
from final
order by 
    status,
    school_level,
    week_of desc 