





with validation_errors as (

    select
        school_year, school_id
    from "dev"."dbt_jordan"."dim_school_stats_by_years"
    group by school_year, school_id
    having count(*) > 1

)

select *
from validation_errors


