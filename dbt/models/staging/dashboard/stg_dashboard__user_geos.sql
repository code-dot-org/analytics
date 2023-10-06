with 
user_geos as (
    select *,
        case when country = 'united states' then 0
             when country <> 'united states' then 1 
             else null 
        end as is_international
    from {{ ref('base_dashboard__user_geos') }}
)

select * 
from user_geos