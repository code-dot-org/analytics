with 
followers as (
    select * 
    from {{ref('base_dashboard__followers')}}
)

select * from followers 