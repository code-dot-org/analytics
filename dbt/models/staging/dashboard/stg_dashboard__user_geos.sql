with 
user_geos as (
    select *,
            
        case when country = 'united states'     then 0
             when country <> 'united states'    then 1 
             else null 
        end as is_international,

        case when country = 'united states'     then 'us'
             when country <> 'united states'    then 'intl'
             else 'missing' 
        end as us_intl,
        
        row_number() over (
            partition by user_id  
            order by user_geo_id desc) as row_number

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
        us_intl,
        -- dates
        created_at,
        updated_at,
        indexed_at
    from user_geos
    where row_number = 1
)

select *
from final 