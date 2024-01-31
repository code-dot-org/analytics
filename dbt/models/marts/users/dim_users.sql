with 
users as (
    select * 
    from {{ ref('stg_dashboard__users') }}
),

user_geos as (
    select * 
    from {{ ref('stg_dashboard__user_geos') }}
),

final as (
    select 
        users.*, 
        ug.is_international
    from users 
    left join user_geos as ug 
        on users.user_id = ug.user_id
)

select *
from final