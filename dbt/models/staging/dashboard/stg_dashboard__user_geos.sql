with 
user_geos as (
    select *,
        case when country = 'united states' then 0
             when country <> 'united states' then 1 
             else null 
        end as is_international
    from {{ ref('base_dashboard__user_geos') }}
),

final as (
    select
        -- pk
        user_id,

        -- geos 
        city,
        state,
        postal_code,
        country,
        is_international,
        
        -- dates
        created_at,
        updated_at,
        indexed_at
    from user_geos
)

select *
from final 