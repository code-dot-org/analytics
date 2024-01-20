
    
    

select
    teacher_id as unique_field,
    count(*) as n_records

from "dev"."dbt_jordan"."dim_teachers"
where teacher_id is not null
group by teacher_id
having count(*) > 1


