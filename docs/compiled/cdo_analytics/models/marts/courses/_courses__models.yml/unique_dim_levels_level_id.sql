
    
    

select
    level_id as unique_field,
    count(*) as n_records

from "dev"."dbt_jordan"."dim_levels"
where level_id is not null
group by level_id
having count(*) > 1


