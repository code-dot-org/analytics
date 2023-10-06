-- (js) NOTE: use `schools` in favor of `school_info` in marts

with 
school_infos as (
    select * 
    from {{ ref('base_dashboard__school_infos')}}
)

select * from school_infos