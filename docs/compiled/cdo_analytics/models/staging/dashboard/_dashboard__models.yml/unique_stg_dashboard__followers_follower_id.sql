
    
    

select
    follower_id as unique_field,
    count(*) as n_records

from "dev"."dbt_allison"."stg_dashboard__followers"
where follower_id is not null
group by follower_id
having count(*) > 1


