
    
    

select
    form_id as unique_field,
    count(*) as n_records

from "dev"."dbt_allison"."dim_forms"
where form_id is not null
group by form_id
having count(*) > 1


