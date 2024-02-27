with 
user_geos as (
    select *,
        row_number() over (partition by user_id order by user_geo_id desc) as row_number,
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

        -- moving this to staging (also avail as a macro)
        case when ug.is_international = 1 then 'international' else 'united states' end as us_intl,
        -- dates
        created_at,
        updated_at,
        indexed_at
        
    from user_geos
    where row_number = 1
)

select *
from final 