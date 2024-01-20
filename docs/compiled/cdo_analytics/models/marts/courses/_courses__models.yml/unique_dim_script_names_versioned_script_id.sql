
    
    

select
    versioned_script_id as unique_field,
    count(*) as n_records

from "dev"."dbt_jordan"."dim_script_names"
where versioned_script_id is not null
group by versioned_script_id
having count(*) > 1


