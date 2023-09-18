
    
    

select
    section_id as unique_field,
    count(*) as n_records

from "dev"."dbt_allison"."dim_sections"
where section_id is not null
group by section_id
having count(*) > 1


