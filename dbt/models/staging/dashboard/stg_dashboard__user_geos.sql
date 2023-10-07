with 
user_geos as (
    select *,
        row_number() over (partition by user_id) as row_number,
        case when country = 'united states' then 0
             when country <> 'united states' then 1 
             else null 
        end as is_international
    from {{ ref('base_dashboard__user_geos') }}
)

select * 
from user_geos
where row_number = 1