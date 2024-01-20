
    
    

select
    user_level_id as unique_field,
    count(*) as n_records

from "dev"."dbt_jordan"."stg_dashboard__user_levels"
where user_level_id is not null
group by user_level_id
having count(*) > 1


