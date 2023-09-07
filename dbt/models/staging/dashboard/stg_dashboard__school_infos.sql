with 
school_infos as (
    select * from {{ ref('base_dashboard__school_infos')}}
)

select * from school_infos