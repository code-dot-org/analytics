





with validation_errors as (

    select
        school_id, school_year
    from "dev"."dbt_jordan"."stg_dashboard__school_stats_by_years"
    group by school_id, school_year
    having count(*) > 1

)

select *
from validation_errors


