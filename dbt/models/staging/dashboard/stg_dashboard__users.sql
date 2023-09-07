with 
users as (
    select * 
    from {{ ref('base_dashboard__users') }}
)

select * from users