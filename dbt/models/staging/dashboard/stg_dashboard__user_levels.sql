with 
user_levels as (
    select * 
    from {{ ref('base_dashboard__user_levels') }}
    where deleted_at is null 
)

select * from user_levels