with schools as (
    select * 
    from {{ ref('base_dashboard__schools') }}
)

select * from schools