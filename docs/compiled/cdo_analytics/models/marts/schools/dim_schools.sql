with 
school_stats as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__school_stats_by_years"
),

school_years as (
    select * 
    from "dev"."dbt_allison"."int_school_years"
),

combined as (

    select school_stats.*,
        school_years.school_year_long,
        school_years.started_at,
        school_years.ended_at,
        
        -- 2nd level aggregations
         -- move to dim_schools
        total_urm_no_tr_students / total_students as pct_urm_no_tr,
        total_urm_students / total_students as pct_urm
    from school_stats
    left join school_years 
        on school_stats.school_year = school_years.school_year
        and school_stats.school_year = school_stats.survey_year
)

select * 
from combined