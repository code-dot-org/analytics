with 
school_districts as (
    select * 
    from {{ ref('base_dashboard__school_districts')}}
)

select * 
from school_districts